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

    #Get the individual occurrence data
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


def mapJSONData(json_dict):
    sdt = datetime.strptime(json_dict['startDateTime'], '%Y-%m-%dT%H:%M:%S%z')
    edt = datetime.strptime(json_dict['endDateTime'], '%Y-%m-%dT%H:%M:%S%z')

    json_dict['vmpJobId'] = json_dict.pop('occurrenceId')
    json_dict['organiserId'] = json_dict.pop('sponsoringOrganizationID')

    json_dict['visibility'] = 'public'
    json_dict['isFull'] = (json_dict['maximumAttendance'] - json_dict['volunteersNeeded']) <= 0
    json_dict['publishedAt'] = json_dict.pop('occurrenceId')

    json_dict['name'] = {'en': json_dict.pop('title')}
    json_dict['description'] = {'en': json_dict['description'].strip()}
    #json_dict['appImage'] = 
    #json_dict['webImage'] = 
    json_dict['url'] = json_dict.pop('detailUrl')

    json_dict['applicationStart'] = json_dict.pop('ocCreatedDate')
    json_dict['applicationEnd'] = (edt + timedelta(days=-1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    json_dict['serviceStart'] = json_dict.pop('startDateTime')
    json_dict['serviceEnd'] = json_dict.pop('endDateTime')
    json_dict['schedules'] = ("\n").join(["Volunteer Service", sdt.strftime("%a, %d %B %Y %I:%M%p"), edt.strftime("%a, %d %B %Y %I:%M%p"), json_dict['locationAddress']])
    json_dict['quota'] = json_dict.pop('maximumAttendance')

    #json_dict['locations'] = no good mapping
    json_dict['causes'] = mapCauses(json_dict['categoryTags'])
    json_dict['recipients'] = mapRecipients(json_dict['populationsServed'])

    json_dict['additionalInfo'] = {
        'locationLatitude': json_dict.pop('Nlatitude'),
        'locationLongitude': json_dict.pop('Nlongitude')
    }

    return json_dict

#key is hohk side (categorytags), value (causes) is JC side
causes_mapping = {
    'Animal Welfare': 'ANIMAL_WELFARE',
    'Arts & Culture': 'ARTS_CULTURE',
    'Civic & Community': 'COMMUNITY_DEVELOPMENT',
    'Maintenance and renovation': 'COMMUNITY_DEVELOPMENT',
    'Disaster and emergency': 'CRISIS_SUPPORT',
    'Diversity and inclusion': 'DIVERSITY_INCLUSION',
    'Training and Empowerment': 'EDUCATION',
    'Education': 'EDUCATION',
    'Environmental Conservation': 'ENVIRONMENT',
    'Health and well-being': 'HEALTH_SPORTS',
    'Food Assistance': 'POVERTY',
    'Awareness and sharing information': 'OTHERS',
    'Support and assistance': 'OTHERS'
}
def mapCauses(json_list):
    new_list = []
    for cause in json_list:
        if cause not in causes_mapping:
            logging.info(f"{cause} has no mapping")
        else:
            new_list.append(causes_mapping[cause])
    return new_list

#key is hohk side (populationsServed), value (recipients) is JC side
recipients_mapping = {
    'Animals': 'ANIMAL',
    'Children and youth': 'CHILDREN_YOUTH',
    'Disadvantaged women': 'WOMEN',
    'Domestic & migrant workers': 'FOREIGN_WORKERS',
    'Elderly': 'ELDERLY',
    'Environment': 'ENVIRONMENT',
    'Ethnic minorities': 'ETHNIC_MINORITY',
    'Families': 'FAMILIES',
    'LGBTQ': 'LGBT',
    'Low income households': 'LOW_INCOME',
    'People experiencing homelessness': 'LOW_INCOME',
    'People with health conditions': 'PATIENTS',
    'People with mental health conditions': 'MENTAL_HEALTH',
    'People with physical disabilities': 'DISABLED',
    'People with special educational needs': 'CHILDREN_YOUTH',
    'Refugees and asylum seekers': 'REFUGEES_ASYLUM'
}
def mapRecipients(json_list):
    new_list = []
    for recipient in json_list:
        if recipient not in recipients_mapping:
            logging.info(f"{recipient} has no mapping")
        else:
            new_list.append(recipients_mapping[recipient])
    return new_list