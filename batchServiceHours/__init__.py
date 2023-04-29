import logging
import os
import pyodbc
import time
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
    success_count = 0
    error_count = 0

    accessToken = getAccessToken()
    if accessToken is None:
        return func.HttpResponse(
            "Could not obtain accessToken",
            status_code=400
        )

    l = []
    rows = cursor.execute(f"SELECT occurrenceId, volunteerId, startDate, endDate, hours FROM serviceHours WHERE status='NOT_SENT'").fetchall()
    for row in rows:
        dict = {'vmpJobId': row.occurrenceId, 'varUserId': row.volunteerId,'startDateTime': (row.startDate.isoformat() + ".000Z"), 'endDateTime': (row.endDate.isoformat() + ".000Z"), 'hour': float(row.hours)}
        linked = isUserLinked(accessToken, row.volunteerId)
        if linked:
            l.append(dict) 
    
    if len(l) == 0:
        return func.HttpResponse(
            "No Service Hours to send",
            status_code=200
        )

    for batch in batched(l, jc_batch_size):
        #send batch
        successes, errors = sendHours(accessToken, batch)
        success_count += successes
        error_count += errors
        batches_sent += 1

    end_time = time.time()
    
    return_message = f"Service Hours total record(s): {total_record_count}. Sent {success_count} with {error_count} errors, in {batches_sent} batches. Time: {str(end_time-start_time)}s"
    logging.info(return_message)
    return func.HttpResponse(
        return_message,
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
        r = requests.post(f"https://{jc_api_url}/{jc_api_login_path}", json={'email': jc_api_username})
        if r.status_code == 200:
            return r.json().get('accessToken')
        else:
            wait = retries * 3 
            time.sleep(wait)
            retries += 1
    
    return None

jc_api_hours_path = os.environ['JC_API_HOURS_PATH']
def sendHours(accessToken, list):
    #logging.info(list)
    retries = 1
    head = {'Authorization': 'Bearer ' + accessToken}
    while retries < 3:
        r = requests.post(f"https://{jc_api_url}/{jc_api_hours_path}", json=list, headers=head)
        if r.status_code == 200:
            dict = r.json()
            logging.info(dict)
            errors = dict.get('error')
            successes = dict.get('success')

            if successes.get('total') > 0:
                for d in successes.get('ids'):
                    varUserId = d.get('varUserId')
                    vmpJobId = d.get('vmpJobId')
                    cursor.execute(f"UPDATE serviceHours SET status='SENT', updatedAt='{time.strftime('%Y-%m-%d %H:%M:%S')}' WHERE occurrenceId='{vmpJobId}' AND volunteerId='{varUserId}'")
                    cnxn.commit()

            if errors.get('total') > 0:
                for d in errors.get('data'):
                    varUserId = d.get('varUserId')
                    vmpJobId = d.get('vmpJobId')
                    message = d.get('message')
                    cursor.execute(f"UPDATE serviceHours SET status='ERRORED', error='{message}', updatedAt='{time.strftime('%Y-%m-%d %H:%M:%S')}' WHERE occurrenceId='{vmpJobId}' AND volunteerId='{varUserId}'")
                    cnxn.commit()

            return (successes.get('total'), errors.get('total'))
        else:
            logging.info("Error in sendHours()")
            logging.info(r.json())
            wait = retries * 3 
            time.sleep(wait)
            retries += 1
    
    logging.info("Failed to send Hours")
    return (0, len(list))

jc_api_volunteer_linkage_path = os.environ['JC_API_VOLUNTEER_LINKAGE_PATH']
def isUserLinked(accessToken, userId):
    retries = 1
    head = {'Authorization': 'Bearer ' + accessToken}
    while retries < 3:
        r = requests.get(f"https://{jc_api_url}/{jc_api_volunteer_linkage_path}?varUserId={userId}", headers=head)
        if r.status_code == 200:
            dict = r.json()
            #logging.info(dict)

            if 'isLink' in dict:
                return dict.get('isLink')
            
            return False
        elif r.status_code == 404:
            #Var user ID not found
            logging.info(f"Var user ID: {userId} not found")
            return False
        else:
            logging.info(r.json())
            wait = retries * 3 
            time.sleep(wait)
            retries += 1
    return False