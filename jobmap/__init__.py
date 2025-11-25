import logging
import os
import azure.functions as func
import requests
import json
import time

hohk_api_url = os.environ["HOHK_API_URL"]
hohk_api_username = os.environ["HOHK_API_USERNAME"]
hohk_api_password = os.environ["HOHK_API_PASSWORD"]


# Creates a list of valid occurrences as of today
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python Jobmap function processed a request.")
    unixtime = int(time.time() * 1000)
    query = "?rows=10000&fl=occurrenceId&group=true&group.field=occurrenceId&group.format=simple&group.main=true&group.limit=1&group.ngroups=true&wt=csv&q=IsOccurrenceActive:true%20AND%20IsOrganizationServedActive:true%20AND%20IsOpportunityActive:true%20AND%20-invitationCode:*"
    query += '%20AND%20scheduleType:"Date%20%26%20Time%20Specific"'

    # Add criteria 1: At least 4 volunteer spots open
    # query += "%20AND%20volunteersNeeded:[4%20TO%20*]"

    # Add criteria 2: Add occurences: now <= (occurence start date) <= 2 months from now
    query += f"%20AND%20endDateTime:[NOW%20TO%20NOW%2B2MONTHS]&NOW={unixtime}"

    r = requests.get(hohk_api_url + query, auth=(hohk_api_username, hohk_api_password))
    to_add = r.text.split()
    to_add.remove("occurrenceId")  # even if list is empty we will still get the header

    return func.HttpResponse(
        # json.dumps(all_occurrences),
        json.dumps(to_add),
        mimetype="application/json",
        status_code=200,
    )
