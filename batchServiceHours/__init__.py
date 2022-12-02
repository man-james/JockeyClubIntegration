import logging
import os
import pyodbc
import time
import datetime
import requests
from itertools import islice
import azure.functions as func

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

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    start_time = time.time()
    jc_batch_size = 100
    total_record_count = 0
    batches_sent = 0
    error_count = 0

    l = []
    rows = cursor.execute(f"SELECT occurrenceId, volunteerId, startDate, endDate, hours FROM serviceHours WHERE status='NOT_SENT'").fetchall()
    for row in rows:
        dict = {'vmpJobId': row.occurrenceId, 'varUserId': row.volunteerId,'startDateTime': row.startDate.isoformat(), 'endDateTime': row.endDate.isoformat(), 'hour': float(row.hours)}
        l.append(dict)
    
    if len(l) == 0:
        return func.HttpResponse(
            "No Service Hours to send",
            status_code=200
        )

    accessToken = getAccessToken()
    if accessToken is None:
        return func.HttpResponse(
            "Could not obtain accessToken",
            status_code=400
        )

    for batch in batched(l, jc_batch_size):
        #send batch
        errors = sendHours(accessToken, batch)
        b = batch.copy()
        if errors: #can be an empty dict {}
            for error in errors['data']:
                #remove failures from b so that SQL isn't updated
                b = [i for i in b if not (i['varUserId'] == error['varUserId'] and i['vmpJobId'] == error['vmpJobId'])]
                error_count += 1
            
        logging.info(b)
        for dict in b:
            cursor.execute(f"UPDATE serviceHours SET status='SENT', updatedAt='{time.strftime('%Y-%m-%d %H:%M:%S')}' where occurrenceID='{dict.get('vmpJobId')}' AND userId='{dict.get('varUserId')}'")
            cnxn.commit()
            total_record_count += 1

        batches_sent += 1

    end_time = time.time()
    
    return func.HttpResponse(
        "Sent " + str(total_record_count) + " record(s) in " + str(batches_sent) + " batches, with " + str(error_count) + " error(s) in " + str(end_time-start_time) + " seconds",
        status_code=200
    )
    
def batched(iterable, n):
    "Batch data into lists of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while (batch := list(islice(it, n))):
        yield batch

jc_api_url = os.environ['JC_API_URL']
jc_api_username = os.environ['JC_API_USERNAME']
jc_api_login_path = os.environ['JC_API_LOGIN_PATH']
def getAccessToken():
    retries = 1
    while retries < 3:
        r = requests.post(f"http://{jc_api_url}/{jc_api_login_path}", json={'email': jc_api_username})
        if r.status_code == 200:
            return r.json().get('accessToken')
        else:
            wait = retries * 3 
            time.sleep(wait)
            retries += 1
    
    return None

jc_api_hours_path = os.environ['JC_API_HOURS_PATH']
def sendHours(accessToken, list):
    logging.info(list)
    retries = 1
    head = {'Authorization': 'Bearer ' + accessToken}
    while retries < 3:
        r = requests.post(f"http://{jc_api_url}/{jc_api_hours_path}", json=list, headers=head)
        if r.status_code == 200:
            #logging.info(r.json())
            return r.json().get('error')
        else:
            #logging.info(r.json())
            wait = retries * 3 
            time.sleep(wait)
            retries += 1
    
    logging.info("Failed to send Hours")
