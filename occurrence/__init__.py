import logging

import azure.functions as func
import os
import requests
from datetime import date, datetime, timedelta
import json
import time
import pytz
import pyodbc

db_url = os.environ['DB_URL']
db = os.environ['DB']
db_username = os.environ['DB_USERNAME']
db_password = os.environ['DB_PASSWORD']
db_driver = os.environ['DB_DRIVER']

for i in range(0, 4):
    while True:
        try:
            cnxn = pyodbc.connect('DRIVER='+db_driver+';SERVER='+db_url+';PORT=1433;DATABASE='+db+';UID=' +
                                  db_username+';PWD='+db_password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
        except pyodbc.Error as ex:
            time.sleep(2.0)
            continue
        break

cursor = cnxn.cursor()

hohk_api_url = os.environ['HOHK_API_URL']
hohk_api_username = os.environ['HOHK_API_USERNAME']
hohk_api_password = os.environ['HOHK_API_PASSWORD']


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python occurrence function processed a request.')

    occurrenceId = req.params.get('occurrenceId')
    if not occurrenceId:
        return func.HttpResponse(
            "Please pass an occurrenceId on the query string or in the request body",
            status_code=400
        )

    query = f"?*:*&rows=200&wt=json&q=occurrenceId:{occurrenceId}"
    r = requests.get(hohk_api_url + query, auth=(hohk_api_username, hohk_api_password))
    json_response = r.json()
    in_solr = False
    if(json_response['response']['numFound'] != 0):
        in_solr = True

    source_job_id = 0
    status = ""
    in_vms = False  # in_db means in VMS
    row = cursor.execute(
        f"SELECT status, sourceJobId FROM occurrences WHERE occurrenceId='{occurrenceId}'").fetchall()
    if row:
        in_vms = True
        status = row[0][0]
        source_job_id = row[0][1]

    if not in_vms and not in_solr:
        #logging.info("Invalid occurrenceId 404")
        return func.HttpResponse(
            "occurrenceId was not found",
            status_code=400
        )

    elif in_vms and not in_solr:
        # it must have been deleted in salesforce
        #cursor.execute("DELETE FROM occurrences where occurrenceId='?'", occurrenceId)
        if status != 'URL_DELETED':
            cursor.execute(
                f"UPDATE occurrences SET status='URL_DELETED', updatedAt='{time.strftime('%Y-%m-%d %H:%M:%S')}' WHERE occurrenceId='{occurrenceId}'")
            cnxn.commit()
            logging.info(f"Removed from HOHK, mark {occurrenceId} as deleted")
        else:
            logging.info(f"{occurrenceId} is already deleted")

        return func.HttpResponse(
            json.dumps(getDeletedObject(source_job_id)),
            mimetype="application/json",
        )

    elif in_vms and in_solr:
        # Was it deleted?  do nothing (WE NEVER READ SOMETHING THAT WAS DELETED)
        if status == 'URL_DELETED':
            return func.HttpResponse(
                json.dumps(getDeletedObject(source_job_id)),
                mimetype="application/json",
            )

        # Should we delete it?
        # Remove from VMS 48 hours before the event start time for Occurrences on Mondays to Saturdays
        # Remove from VMS 72 hours before the event start time for Occurrences that run on Sundays
        # Mark as deleted in DB and return
        expiry_datetime = json_response['response']['docs'][0]['opportunityExpires']
        # print(expiry_datetime)
        if datetime.strptime(expiry_datetime, '%Y-%m-%dT%H:%M:%S%z') < pytz.utc.localize(datetime.now()):
            cursor.execute(
                f"UPDATE occurrences SET status='URL_DELETED', updatedAt='{time.strftime('%Y-%m-%d %H:%M:%S')}' WHERE occurrenceId='{occurrenceId}'")
            cnxn.commit()
            return func.HttpResponse(
                json.dumps(getDeletedObject(source_job_id)),
                mimetype="application/json",
            )

        # No need to delete it. Update it
        # No need to set the DB status
        return func.HttpResponse(
            json.dumps(getObject(source_job_id, "URL_UPDATED",
                                 json_response['response']['docs'])),
            mimetype="application/json",
        )

    else:  # not in VMS but in solr
        # URL_ADDED first time
        cursor.execute(f"INSERT INTO occurrences(occurrenceId, createdAt, status) VALUES (?, ?, ?)",
                       occurrenceId, time.strftime('%Y-%m-%d %H:%M:%S'), "URL_ADDED")
        cnxn.commit()
        # Get the sourceJobId
        row = cursor.execute(
            f"SELECT sourceJobId FROM occurrences WHERE occurrenceId='{occurrenceId}'").fetchall()
        if row:
            source_job_id = row[0][0]
        else:
            logging.error("Source Job Id not found, server error 500")
            return func.HttpResponse(
                "Source Job Id was not found",
                status_code=500
            )

        return func.HttpResponse(
            json.dumps(getObject(source_job_id, "URL_ADDED",
                                 json_response['response']['docs'])),
            mimetype="application/json",
        )


def getDeletedObject(source_job_id):
    return_obj = [{
        "object": "job",
        "type": "URL_DELETED",
        "locale": "en",
        "source": "handsonhk",
        "data": {"source_job_id": source_job_id}
    }, {
        "object": "job",
        "type": "URL_DELETED",
        "locale": "zh_HK",
        "source": "handsonhk",
        "data": {"source_job_id": source_job_id}
    }, {
        "object": "job",
        "type": "URL_DELETED",
        "locale": "zh_CN",
        "source": "handsonhk",
        "data": {"source_job_id": source_job_id}
    }]
    return return_obj


def getObject(source_job_id, status, json_list):
    # Map the fields to VMS format
    # print(status)
    # print(json_list)
    have_en = False
    have_hk = False
    en_dict = {}
    zh_HK_dict = {}
    #zh_CN = zh_HK
    for json_dict in json_list:
        if json_dict['Language'] == 'English':
            have_en = True
            en_dict = mapJSONData(json_dict, source_job_id)

        elif json_dict['Language'] == 'Chinese':
            # build simplified and traditional
            have_hk = True
            zh_HK_dict = mapJSONData(json_dict, source_job_id)

    if have_en and not have_hk:
        # copy en in simpl and trad
        zh_HK_dict = en_dict

    if not have_en and have_hk:
        # copy trad into en
        en_dict = zh_HK_dict

    return_obj = [{
        "object": "job",
        "type": status,
        "locale": "en",
        "source": "handsonhk",
        "data": en_dict
    }, {
        "object": "job",
        "type": status,
        "locale": "zh_HK",
        "source": "handsonhk",
        "data": zh_HK_dict
    }, {
        "object": "job",
        "type": status,
        "locale": "zh_CN",
        "source": "handsonhk",
        "data": zh_HK_dict
    }]
    return return_obj


def mapJSONData(json_dict, source_job_id):
    sdt = datetime.strptime(json_dict['startDateTime'], '%Y-%m-%dT%H:%M:%S%z')
    edt = datetime.strptime(json_dict['endDateTime'], '%Y-%m-%dT%H:%M:%S%z')

    json_dict['source_job_id'] = source_job_id
    json_dict['name'] = json_dict.pop('title')
    json_dict['ref_code'] = json_dict.pop('occurrenceId')
    json_dict['overall_start_date'] = json_dict.pop('startDateTime')
    json_dict['overall_end_date'] = json_dict.pop('endDateTime')
    json_dict['application_start_date'] = json_dict.pop('ocCreatedDate')
    json_dict['application_end_date'] = (
        edt + timedelta(days=-1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    json_dict['locations'] = [json_dict.pop('locationAddress')]
    json_dict['tags_roles'] = json_dict.pop('categoryTags')  # already an array
    # already an array, or maybe 'audienceTags'
    json_dict['tags_recipients'] = json_dict.pop('populationsServed')
    # description is named the same
    json_dict['description'] = json_dict['description'].strip()
    json_dict['contact_details'] = {
        'name': json_dict['coordinatorName'], 'email': json_dict['coordinatorEmail']}
    json_dict['schedules_display'] = {
        'SERVICE_SCHEDULE': {
            'title': 'Service',
            'schedules': [{
                'datetime': [sdt.strftime("%a, %d %B %Y %I:%M%p"), "To", edt.strftime("%a, %d %B %Y %I:%M%p")],
                'address': json_dict['locations'][0]
            }]
        }
    }
    json_dict['ngo'] = {
        'name': json_dict['sponsoringOrganizationName'],
        'ngo_website': json_dict['sponsoringOrganizationUrl']
    }
    json_dict['application_uri'] = json_dict.pop('detailUrl')
    json_dict['total_quota'] = json_dict.pop('maximumAttendance')
    json_dict['available_quota'] = json_dict.pop('volunteersNeeded')
    if 'skills' in json_dict:
        json_dict['required_skills'] = json_dict.pop('skills')
    if 'Nlatitude' in json_dict and 'Nlongitude' in json_dict:
        json_dict['lat_long'] = {
            'lat': json_dict['Nlatitude'], 'long': json_dict['Nlongitude']}
    json_dict['media_uri'] = json_dict.pop('voThumbnailUrl')
    return json_dict
