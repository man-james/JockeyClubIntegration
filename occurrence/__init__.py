import logging

import azure.functions as func
import os
import requests
import base64
from datetime import datetime, timedelta
import json
import time
import pyodbc

#db_url = os.environ['DB_URL']
#db = os.environ['DB']
#db_username = os.environ['DB_USERNAME']
#db_password = os.environ['DB_PASSWORD']
#db_driver = os.environ['DB_DRIVER']
#cnxn = None
#for i in range(0, 4):
#    while True:
#        try:
#            cnxn = pyodbc.connect('DRIVER='+db_driver+';SERVER='+db_url+';PORT=1433;DATABASE='+db+';UID=' +
#                                  db_username+';PWD='+db_password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
#        except pyodbc.Error as ex:
#            time.sleep(2.0)
#            continue
#        break

#cursor = cnxn.cursor()

hohk_api_url = os.environ['HOHK_API_URL']
hohk_api_username = os.environ['HOHK_API_USERNAME']
hohk_api_password = os.environ['HOHK_API_PASSWORD']
select_query = "fl=occurrenceId,sponsoringOrganizationID,maximumAttendance,volunteersNeeded,voThumbnailUrl,voCreatedDate,title,description,detailUrl,ocCreatedDate,startDateTime,endDateTime,locationAddress,categoryTags,populationsServed,Nlatitude,Nlongitude,Language"
#returns a json for a single occurrence in JC format
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python occurrence function processed a request.')

    occurrenceId = req.params.get('occurrenceId')
    comma_count = occurrenceId.count(',') + 1
    occurrenceIds = occurrenceId.replace(',', ' ')
    if not occurrenceId:
        return func.HttpResponse(
            "Please pass one of occurrenceId or comma separated occurrenceId on the query string or in the request body",
            status_code=400
        )

    #(occurenceId1 occurenceId2) the parenthesis allows for SQL IN style query
    query = f"?*:*&rows=200&wt=json&q=occurrenceId:({occurrenceIds})&{select_query}"
    r = requests.get(hohk_api_url + query, auth=(hohk_api_username, hohk_api_password))
    json_response = r.json()

    #logging.info(f"Found {json_response['response']['numFound']} results for {comma_count} occurrences.")
    
    #need to actually loop based on query param and group based on matching occurrenceIds
    return_occurrences = []
    for id in occurrenceId.split(','):
        matches = [d for d in json_response['response']['docs'] if d['occurrenceId'] == id]
        #logging.info(f"Found {len(matches)} matches for occurrence {id}")
        if len(matches) > 0:
            d2 = getObject(matches)
            return_occurrences.append(d2)

    return func.HttpResponse(
        json.dumps(return_occurrences),
        mimetype="application/json",
    )

def getObject(json_list):
    # Map the fields to VMS format
    #logging.info(json_list[0].keys())

    #what language is at index 0?
    eng_dict = None
    chi_dict = None
    if json_list[0]['Language'] == 'English':
        eng_dict = json_list[0]
        if len(json_list) > 1:
            chi_dict = json_list[1]
    else:
        chi_dict = json_list[0]
        if len(json_list) > 1:
            eng_dict = json_list[1]

    dict = mapJSONData(eng_dict, chi_dict)
    return dict

#pass in None if that language doesn't exist
def mapJSONData(json_dict_eng, json_dict_chi):
    json_dict = {}
    primary_dict = json_dict_eng
    has_english = True if json_dict_eng is not None else False
    has_chinese = True if json_dict_chi is not None else False
    if has_english == False:
        primary_dict = json_dict_chi

    sdt = datetime.strptime(primary_dict['startDateTime'], '%Y-%m-%dT%H:%M:%S%z')
    edt = datetime.strptime(primary_dict['endDateTime'], '%Y-%m-%dT%H:%M:%S%z')

    json_dict['vmpJobId'] = primary_dict['occurrenceId']
    json_dict['organiserId'] = primary_dict['sponsoringOrganizationID']

    json_dict['visibility'] = 'public'
    json_dict['isFull'] = (primary_dict['maximumAttendance'] - primary_dict['volunteersNeeded']) <= 0
    json_dict['publishedAt'] = primary_dict['voCreatedDate']

    name = {}
    if has_english:
        name['en'] = json_dict_eng['title']

    if has_chinese:
        name['zh'] = json_dict_chi['title']

    json_dict['name'] = name

    description = {}
    if has_english:
        description['en'] = json_dict_eng['description'].strip()

    if has_chinese:
        description['zh'] = json_dict_chi['description'].strip()

    json_dict['description'] = description

    b64 = ""
    try: 
        b64 = getBase64String(primary_dict['voThumbnailUrl']) #these are always square? 350x350
    except:
        b64 = getBase64String("https://hocps.blob.core.windows.net/00006b/images/opp_icons/others.png")
        #This is Other

    json_dict['appImage'] = b64 #Base64 image string 4:3
    json_dict['webImage'] = b64 #supposed to be 16:9
    json_dict['url'] = primary_dict['detailUrl']

    json_dict['applicationStart'] = primary_dict['ocCreatedDate']
    json_dict['applicationEnd'] = (edt + timedelta(days=-1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    json_dict['serviceStart'] = primary_dict['startDateTime']
    json_dict['serviceEnd'] = primary_dict['endDateTime']

    schedules = {}
    if has_english:
        schedules['en'] = ("\n").join(["Volunteer Service", sdt.strftime("%a, %d %B %Y %I:%M%p"), edt.strftime("%a, %d %B %Y %I:%M%p"), json_dict_eng['locationAddress']])

    if has_chinese:
        schedules['zh'] = ("\n").join(["義工服務", sdt.strftime("%a, %d %B %Y %I:%M%p"), edt.strftime("%a, %d %B %Y %I:%M%p"), json_dict_chi['locationAddress']])

    json_dict['schedules'] = schedules
    json_dict['quota'] = primary_dict['maximumAttendance']

    #json_dict['locations'] = no good mapping
    if "categoryTags" in primary_dict:
        json_dict['causes'] = mapCauses(primary_dict['categoryTags'])

    if "populationsServed" in primary_dict:
        json_dict['recipients'] = mapRecipients(primary_dict['populationsServed'])

    if "Nlatitude" in primary_dict and "Nlongitude" in primary_dict:
        json_dict['additionalInfo'] = {
            'locationLatitude': primary_dict['Nlatitude'],
            'locationLongitude': primary_dict['Nlongitude']
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
            logging.info(f"Cause '{cause}' has no mapping")
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
            logging.info(f"Recipient '{recipient}' has no mapping")
        else:
            new_list.append(recipients_mapping[recipient])
    return new_list

def getBase64String(url):
    return base64.b64encode(requests.get(url).content).decode('utf-8')

