import logging
import os
import azure.functions as func
import pyodbc
import requests
from datetime import date
import time
from itertools import islice
import json
import base64

db_url = os.environ["DB_URL"]
db = os.environ["DB"]
db_username = os.environ["DB_USERNAME"]
db_password = os.environ["DB_PASSWORD"]
db_driver = os.environ["DB_DRIVER"]

default_image_url = os.environ["DEFAULT_IMAGE_URL"]

# serverless DB retry
for i in range(0, 4):
    while True:
        try:
            cnxn = pyodbc.connect(
                "DRIVER="
                + db_driver
                + ";SERVER="
                + db_url
                + ";PORT=1433;DATABASE="
                + db
                + ";UID="
                + db_username
                + ";PWD="
                + db_password
                + ";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
            )
        except pyodbc.Error as ex:
            time.sleep(2.0)
            continue
        break

cursor = cnxn.cursor()


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Call batchOccurences function.")

    start_time = time.time()

    rows = cursor.execute(f"SELECT json FROM occurrences WHERE send=1").fetchall()
    list_of_json_dict = [json.loads(row.json) for row in rows]

    jc_batch_size = 10
    total_record_count = len(rows)
    batches_sent = 0
    success_count = 0
    error_count = 0

    logging.info(f"Received {total_record_count} results to send")

    # need to add back the b64 images
    new_list = []
    for dict in list_of_json_dict:
        b64 = ""
        try:
            b64 = getBase64String(dict["appImage"])  # these are always square? 350x350
        except:
            b64 = getBase64String(default_image_url)

        dict["appImage"] = b64  # Base64 image string 4:3
        dict["webImage"] = b64  # supposed to be 16:9
        new_list.append(dict)

    accessToken = getAccessToken()
    if accessToken is None:
        logging.info("Could not obtain accessToken.")
        return func.HttpResponse("Could not obtain accessToken", status_code=400)

    for batch in batched(new_list, jc_batch_size):
        # send batch
        successes, errors = upsertVOs(accessToken, batch)
        success_count += successes
        error_count += errors
        batches_sent += 1

    end_time = time.time()

    return_message = f"Upsert VOs total record(s): {total_record_count}. Sent {success_count} with {error_count} errors, in {batches_sent} batches. Time: {str(end_time-start_time)}s"
    logging.info(return_message)
    return func.HttpResponse(return_message, status_code=200)


def batched(iterable, n):
    # "Batch data into lists of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := list(islice(it, n)):
        yield batch


jc_api_url = os.environ["JC_API_URL"]
jc_api_username = os.environ["JC_API_USERNAME"]
jc_api_login_path = os.environ["JC_API_LOGIN_PATH"]


def getAccessToken():
    retries = 1
    while retries < 3:
        r = requests.post(
            f"https://{jc_api_url}/{jc_api_login_path}", json={"email": jc_api_username}
        )
        logging.info(r)
        if r.status_code == 200:
            return r.json().get("accessToken")
        else:
            wait = retries * 3
            time.sleep(wait)
            retries += 1

    return None


jc_api_upsert_path = os.environ["JC_API_UPSERT_PATH"]


def upsertVOs(accessToken, list):
    retries = 1
    head = {"Authorization": "Bearer " + accessToken}

    while retries < 3:
        r = requests.post(
            f"https://{jc_api_url}/{jc_api_upsert_path}", json=list, headers=head
        )
        if r.status_code == 200:
            dict = r.json()
            logging.info(dict)
            errors = dict.get("error")
            successes = dict.get("success")

            if successes.get("total") > 0:
                ids = successes.get("ids")
                sql_ids = (",").join(f"'{w}'" for w in ids)
                cursor.execute(
                    f"UPDATE occurrences SET send=0, status='SENT', error='', updatedAt='{time.strftime('%Y-%m-%d %H:%M:%S')}' WHERE occurrenceId IN ({sql_ids})"
                )
                cnxn.commit()

            if errors.get("total") > 0:
                for d in errors.get("data"):
                    id = d.get("id")
                    message = d.get("message")
                    cursor.execute(
                        f"UPDATE occurrences SET send=0, status='ERRORED', error='{message}', updatedAt='{time.strftime('%Y-%m-%d %H:%M:%S')}' WHERE occurrenceId='{id}'"
                    )
                    cnxn.commit()

            return (successes.get("total"), errors.get("total"))
        else:
            logging.info("Error in upsertVOs()")
            logging.info(r.status_code)
            logging.info(r.json())
            wait = retries * 3
            time.sleep(wait)
            retries += 1

    logging.info("Failed to upsert")
    return (0, len(list))


base64_image_cache = {}


def getBase64String(url):
    if url in base64_image_cache:
        return base64_image_cache[url]

    b64 = base64.b64encode(requests.get(url).content).decode("utf-8")
    base64_image_cache[url] = b64
    return b64
