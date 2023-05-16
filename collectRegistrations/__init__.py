import logging
import os
import pyodbc
import time
import json
import xmltodict
import requests

import azure.functions as func

db_url = os.environ["DB_URL"]
db = os.environ["DB"]
db_username = os.environ["DB_USERNAME"]
db_password = os.environ["DB_PASSWORD"]
db_driver = os.environ["DB_DRIVER"]

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

xml_res = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <ns3:notificationResponse xmlns:ns3="http://soap.sforce.com/2005/09/outbound">
            <ns3:Ack>REPLACE</ns3:Ack>
        </ns3:notificationResponse>
    </soapenv:Body>
</soapenv:Envelope>"""


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")
    # logging.info(req.get_body())

    try:
        dict = xmltodict.parse(req.get_body())
        json_data = json.dumps(dict)
        connection_data = dict["soapenv:Envelope"]["soapenv:Body"]["notifications"][
            "Notification"
        ]["sObject"]
        hohk_id = connection_data.get("sf:Id")
        jcvar_id = connection_data.get("sf:JCVAR_UserId__c")
        cursor.execute(
            f"INSERT INTO registrations(hohkId, jcvarId, status, xml, createdAt, updatedAt, error) VALUES (?, ?, ?, ?, ?, ?, ?)",
            hohk_id,
            jcvar_id,
            "NOT_SENT",
            json_data,
            time.strftime("%Y-%m-%d %H:%M:%S"),
            None,
            None,
        )
        cnxn.commit()

        accessToken = getAccessToken()
        if accessToken is None:
            cursor.close()
            cnxn.close()
            return func.HttpResponse(
                xml_res.replace("REPLACE", "false"),
                status_code=200,
                headers={"content-type": "application/xml"},
            )

        linkUser(accessToken, True, jcvar_id)

        cursor.close()
        cnxn.close()
        return func.HttpResponse(
            xml_res.replace("REPLACE", "true"),
            status_code=200,
            headers={"content-type": "application/xml"},
        )
    except Exception as err:
        logging.error(f"Unexpected {err=}, {type(err)=}")

    cursor.close()
    cnxn.close()
    return func.HttpResponse(
        xml_res.replace("REPLACE", "false"),
        status_code=200,
        headers={"content-type": "application/xml"},
    )


jc_api_url = os.environ["JC_API_URL"]
jc_api_username = os.environ["JC_API_USERNAME"]
jc_api_login_path = os.environ["JC_API_LOGIN_PATH"]


def getAccessToken():
    retries = 1
    while retries < 3:
        r = requests.post(
            f"https://{jc_api_url}/{jc_api_login_path}", json={"email": jc_api_username}
        )
        if r.status_code == 200:
            return r.json().get("accessToken")
        else:
            wait = retries * 3
            time.sleep(wait)
            retries += 1

    return None


jc_api_volunteer_link_path = os.environ["JC_API_VOLUNTEER_LINK_PATH"]


# currently never unlink
def linkUser(accessToken, link, userId):
    retries = 1
    head = {"Authorization": "Bearer " + accessToken}
    while retries < 3:
        r = requests.post(
            f"https://{jc_api_url}/{jc_api_volunteer_link_path}",
            json={"varUserId": userId, "isLink": link},
            headers=head,
        )
        if r.status_code == 200:
            dict = r.json()
            logging.info(dict)

            if "isLink" in dict:
                link_text = "LINKED" if link else "UNLINKED"
                cursor.execute(
                    f"UPDATE registrations SET status='{link_text}', updatedAt='{time.strftime('%Y-%m-%d %H:%M:%S')}' WHERE jcvarId='{userId}'"
                )
                cnxn.commit()
            else:
                message = dict.get("message")
                cursor.execute(
                    f"UPDATE registrations SET error='{message}', updatedAt='{time.strftime('%Y-%m-%d %H:%M:%S')}' WHERE jcvarId='{userId}'"
                )
                cnxn.commit()
            return
        elif r.status_code == 404:
            # Var user ID not found
            logging.info(f"Var user ID: {userId} not found")
            return
        else:
            logging.info(r.json())
            wait = retries * 3
            time.sleep(wait)
            retries += 1
    return
