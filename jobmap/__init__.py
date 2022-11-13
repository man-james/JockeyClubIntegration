import logging
import os
import azure.functions as func
import pyodbc
import requests
from datetime import date
import json
import time

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

hohk_api_url = os.environ['HOHK_API_URL']
hohk_api_username = os.environ['HOHK_API_USERNAME']
hohk_api_password = os.environ['HOHK_API_PASSWORD']

#Creates a list of valid occurrences as of today
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python Jobmap function processed a request.')
    unixtime = int(time.time()*1000) 
    query = "?rows=10000&fl=occurrenceId&group=true&group.field=occurrenceId&group.format=simple&group.main=true&group.limit=1&group.ngroups=true&wt=csv&q=IsOccurrenceActive:true%20AND%20IsOrganizationServedActive:true%20AND%20IsOpportunityActive:true"
    query += '%20AND%20scheduleType:"Date%20%26%20Time%20Specific"'
    
    #Add criteria 1: At least 4 volunteer spots open
    query += "%20AND%20volunteersNeeded:[4%20TO%20*]"

    #Add criteria 2: Add occurences: now <= (occurence start date) <= 2 months from now 
    query += f"%20AND%20endDateTime:[NOW%20TO%20NOW%2B2MONTHS]&NOW={unixtime}"
    
    r = requests.get(hohk_api_url + query, auth=(hohk_api_username, hohk_api_password))
    to_add = r.text.split()
    to_add.remove('occurrenceId') #even if list is empty we will still get the header

    #Also, add things that need changing in VMS?
    cursor = cnxn.cursor()
    rows = cursor.execute("SELECT occurrenceId FROM occurrences where status='URL_ADDED'").fetchall()
    flattened_rows = [item[0] for item in rows]

    all_occurrences = mergeNoDuplicates(to_add, flattened_rows)
    return func.HttpResponse(
        json.dumps(all_occurrences), 
        mimetype="application/json",
        status_code=200
    )

def mergeNoDuplicates(iterable_1, iterable_2):
    myset = set(iterable_1).union(set(iterable_2))
    return sorted(list(myset))