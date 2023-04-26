import datetime
import logging
import os
import requests
import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    cache_occurrences_url = os.environ['THIS_API_URL'] + '/cacheoccurrences?code=' + os.environ['CACHE_OCCURRENCES_FUNCTION_CODE']

    r = requests.get(cache_occurrences_url)

    if r.status_code == 200:
        logging.info(r.text)
    else:
        logging.error(f"Received status code {r.status_code}")