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
    logging.info('Call batchOccurences function.')

    start_time = time.time()

    r = requests.get(jobmap_url)
    json_response = r.json()
    batch_size = 100
    batch = []
    total_record_count = 0
    batches_sent = 0
    
    for i, occurrenceId in enumerate(json_response):
        r2 = requests.get(occurrence_url_prefix + occurrenceId)
        if r2.status_code == 200:
            dict = r2.json()
            if len(dict) == 0:
                logging.info("Occurrence: " + occurrenceId + " resulted in an empty response")
            else:
                total_record_count += 1
                batch.append(r2.json())
        else: #other error codes
            logging.error("Occurrence: " + occurrenceId + " gave response code: " + str(r2.status_code))
            logging.error(occurrence_url_prefix + occurrenceId)

        if len(batch) == batch_size:
            #send batch
            batch = []
            batches_sent += 1

    #send last batch if not empty
    if len(batch) > 0:
        batches_sent += 1
        #send batch

    end_time = time.time()

    return func.HttpResponse(
        "Sent " + str(total_record_count) + " record(s) in " + str(batches_sent) + " batches, in " + str(end_time-start_time) + " seconds",
        status_code=200
    )