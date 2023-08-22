import logging
import os
import pyodbc
import time
import json
import xmltodict

import azure.functions as func

db_url = os.environ["DB_URL"]
db = os.environ["DB"]
db_username = os.environ["DB_USERNAME"]
db_password = os.environ["DB_PASSWORD"]
db_driver = os.environ["DB_DRIVER"]


xml_res = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <ns3:notificationResponse xmlns:ns3="http://soap.sforce.com/2005/09/outbound">
            <ns3:Ack>REPLACE</ns3:Ack>
        </ns3:notificationResponse>
    </soapenv:Body>
</soapenv:Envelope>"""


def main(req: func.HttpRequest) -> func.HttpResponse:
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

    logging.info("Python HTTP trigger function processed a request.")
    # logging.info(req.get_body())

    try:
        dict = xmltodict.parse(req.get_body())
        json_data = json.dumps(dict)
        logging.info(json_data)
        connection_data = {}

        # this can either be a list if multiple entries are marked as served when the form is sent
        is_list = isinstance(
            dict["soapenv:Envelope"]["soapenv:Body"]["notifications"]["Notification"],
            list,
        )

        all_connection_data = []
        if is_list:
            all_connection_data = dict["soapenv:Envelope"]["soapenv:Body"][
                "notifications"
            ]["Notification"]
        else:
            all_connection_data.append(
                dict["soapenv:Envelope"]["soapenv:Body"]["notifications"][
                    "Notification"
                ]
            )

        # logging.info(connection_data)
        for entry in all_connection_data:
            connection_data = entry["sObject"]
            attendance_status = connection_data.get("sf:HOC__Attendance_Status__c")
            # logging.info(f"Attendence Status {attendance_status}")
            if attendance_status == "Attended (and Hours Verified)":
                occurrenceId = connection_data.get("sf:HOC__Occurrence__c")  # vmpJobId
                userId = connection_data.get(
                    "sf:HOC_Contact_JCVAR_UserId__c"
                )  # varUserId
                sdt = connection_data.get(
                    "sf:HOC_Occurrence_Start_Date_Time__c"
                )  # startDateTime In ISO 8601 datetime format with UTC.
                edt = connection_data.get(
                    "sf:HOC_Occurrence_End_Date_Time__c"
                )  # endDateTime
                hours = float(
                    connection_data.get("sf:HOC__Number_Hours_Served__c")
                )  # hour

                # logging.info(f"{occurrenceId} {userId} {sdt} {edt} {hours}")
                cursor.execute(
                    f"INSERT INTO serviceHours(occurrenceId, volunteerId, startDate, endDate, hours, status, xml, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    occurrenceId,
                    userId,
                    sdt,
                    edt,
                    hours,
                    "NOT_SENT",
                    json_data,
                    time.strftime("%Y-%m-%d %H:%M:%S"),
                    None,
                )
                cnxn.commit()

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
