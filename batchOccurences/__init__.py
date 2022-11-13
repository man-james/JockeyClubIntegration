import logging
import os
import azure.functions as func
import pyodbc
import requests
from datetime import date
import json
import time
import math

db_url = os.environ['DB_URL']
db = os.environ['DB']
db_username = os.environ['DB_USERNAME']
db_password = os.environ['DB_PASSWORD']
db_driver = os.environ['DB_DRIVER']

#serverless DB retry
for i in range(0, 4):
    while True:
        try:
            cnxn = pyodbc.connect('DRIVER='+db_driver+';SERVER='+db_url+';PORT=1433;DATABASE='+db+';UID='+db_username+';PWD='+db_password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
        except pyodbc.Error as ex:
            time.sleep(2.0)
            continue
        break

cursor = cnxn.cursor()

hohk_api_url = os.environ['HOHK_API_URL']
hohk_api_username = os.environ['HOHK_API_USERNAME']
hohk_api_password = os.environ['HOHK_API_PASSWORD']

occurrence_url_prefix = os.environ['THIS_API_URL'] + '/occurrence?code=' + os.environ['OCCURRENCE_FUNCTION_CODE'] + '&occurrenceId='
jobmap_url = os.environ['THIS_API_URL'] + '/jobmap?code=' + os.environ['JOBMAP_FUNCTION_CODE']

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Call jobmap function.')

    r = requests.get(jobmap_url)
    json_response = r.json()
    batch_size = 100
    batches = [[] for i in range(math.ceil(len(json_response)/batch_size))] #an array where each value has another list with up to 100 elements
    batch_index = 0
    
    for i, occurrenceId in enumerate(json_response):
        r2 = requests.get(occurrence_url_prefix + occurrenceId)
        batches[batch_index].append(r2)

        if i != 0 and i % batch_size == 0:
            batch_index += 1

    logging.info(batches)

    return func.HttpResponse(
        "Test SUCCESS",
        status_code=200
    )