import logging
import os
import azure.functions as func
import requests
import time
import pyodbc

db_url = os.environ["DB_URL"]
db = os.environ["DB"]
db_username = os.environ["DB_USERNAME"]
db_password = os.environ["DB_PASSWORD"]
db_driver = os.environ["DB_DRIVER"]
jc_api_url = os.environ["JC_API_URL"]
jc_api_username = os.environ["JC_API_USERNAME"]
jc_api_login_path = os.environ["JC_API_LOGIN_PATH"]
jc_api_unlist_path = os.environ["JC_API_UNLIST_PATH"]

def getAccessToken():
    retries = 1
    while retries < 3:
        r = requests.post(
            f"https://{jc_api_url}/{jc_api_login_path}", json={"email": jc_api_username}
        )
        # logging.info(r)
        if r.status_code == 200:
            return r.json().get("accessToken")
        else:
            wait = retries * 3
            time.sleep(wait)
            retries += 1

    return None

def unlistOccurrence(accessToken, occurrenceId, cnxn, cursor):
    retries = 1
    head = {"Authorization": "Bearer " + accessToken}

    json_body = [{
        "vmpJobId": occurrenceId,
        "visibility": "unlisted"
    }]

    while retries < 3:
        r = requests.post(
            f"https://{jc_api_url}/{jc_api_unlist_path}", json=json_body, headers=head
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
                    f"UPDATE occurrences SET send=0, status='UNLISTED', error='', updatedAt='{time.strftime('%Y-%m-%d %H:%M:%S')}' WHERE occurrenceId IN ({sql_ids})"
                )
                cnxn.commit()

            if errors.get("total") > 0:
                for d in errors.get("data"):
                    id = d.get("id")
                    message = d.get("message")
                    #cursor.execute(
                    #    f"UPDATE occurrences SET send=0, status='ERRORED', error='{message}', updatedAt='{time.strftime('%Y-%m-%d %H:%M:%S')}' WHERE occurrenceId='{id}'"
                    #)
                    #cnxn.commit()

            return (successes.get("total"), errors.get("total"))
        else:
            logging.info("Error in unlist()")
            logging.info(r.status_code)
            outgoing_ip = requests.get("https://api.ipify.org/?format=json").json()[
                "ip"
            ]
            logging.info(outgoing_ip)
            logging.info(r.content)
            logging.info(r.json())
            wait = retries * 3
            time.sleep(wait)
            retries += 1

    logging.info("Failed to unlist")
    return (0, len(list))


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Call unlist function.")

    # DB retry
    cnxn = None
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
    if cnxn == None:
        logging.info("Could not connect to database")
        return func.HttpResponse("Could not obtain accessToken", status_code=400)

    cursor = cnxn.cursor()

    start_time = time.time()

    occurrenceId = req.params.get("occurrenceId")
    if not occurrenceId:
        return func.HttpResponse(
            "Please pass one of occurrenceId on the query string",
            status_code=400,
        )

    accessToken = getAccessToken()
    if accessToken is None:
        logging.info("Could not obtain accessToken.")
        cursor.close()
        cnxn.close()
        return func.HttpResponse("Could not obtain accessToken", status_code=400)
    successes, errors = unlistOccurrence(accessToken, occurrenceId, cnxn, cursor)

    end_time = time.time()

    return_message = f"Unlisted {occurrenceId}. Time: {str(end_time-start_time)}s"
    logging.info(return_message)
    cursor.close()
    cnxn.close()
    return func.HttpResponse(return_message, status_code=200)

