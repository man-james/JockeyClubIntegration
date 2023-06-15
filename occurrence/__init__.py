import logging

import azure.functions as func
import os
import requests
from datetime import datetime, timedelta
import pytz
import json

hohk_api_url = os.environ["HOHK_API_URL"]
hohk_api_username = os.environ["HOHK_API_USERNAME"]
hohk_api_password = os.environ["HOHK_API_PASSWORD"]
select_query = "fl=occurrenceId,sponsoringOrganizationID,maximumAttendance,volunteersNeeded,voThumbnailUrl,voCreatedDate,title,description,detailUrl,ocCreatedDate,startDateTime,endDateTime,locationAddress,categoryTags,populationsServed,Nlatitude,Nlongitude,Language"
default_image_url = os.environ["DEFAULT_IMAGE_URL"]


# returns a json for a single occurrence in JC format
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python occurrence function processed a request.")

    occurrenceId = req.params.get("occurrenceId")
    comma_count = occurrenceId.count(",") + 1
    occurrenceIds = occurrenceId.replace(",", " ")
    if not occurrenceId:
        return func.HttpResponse(
            "Please pass one of occurrenceId or comma separated occurrenceId on the query string or in the request body",
            status_code=400,
        )

    # (occurenceId1 occurenceId2) the parenthesis allows for SQL IN style query
    query = f"?*:*&rows=200&wt=json&q=occurrenceId:({occurrenceIds})&{select_query}"
    r = requests.get(hohk_api_url + query, auth=(hohk_api_username, hohk_api_password))
    json_response = r.json()

    logging.info(
        f"Found {json_response['response']['numFound']} results for {comma_count} occurrences."
    )

    # need to actually loop based on query param and group based on matching occurrenceIds
    return_occurrences = []
    full_list = json_response["response"]["docs"]
    for id in occurrenceId.split(","):
        matches = [d for d in full_list if d.get("occurrenceId") == id]

        # logging.info(f"Found {len(matches)} matches for occurrence {id}")
        if len(matches) > 0:
            d2 = getObject(matches)
            return_occurrences.append(d2)
        else:
            logging.info(f"No matches found for occurrence {id}")

    return func.HttpResponse(
        json.dumps(return_occurrences),
        mimetype="application/json",
    )


def getObject(json_list):
    # Map the fields to VMS format
    # logging.info(json_list[0].keys())

    # what language is at index 0?
    eng_dict = None
    chi_dict = None
    if json_list[0]["Language"] == "English":
        eng_dict = json_list[0]
        if len(json_list) > 1:
            chi_dict = json_list[1]
    else:
        chi_dict = json_list[0]
        if len(json_list) > 1:
            eng_dict = json_list[1]

    dict = mapJSONData(eng_dict, chi_dict)
    return dict


# pass in None if that language doesn't exist
def mapJSONData(json_dict_eng, json_dict_chi):
    json_dict = {}
    primary_dict = json_dict_eng
    has_english = True if json_dict_eng is not None else False
    has_chinese = True if json_dict_chi is not None else False
    if has_english == False:
        primary_dict = json_dict_chi

    sdt = datetime.strptime(primary_dict["startDateTime"], "%Y-%m-%dT%H:%M:%S%z")
    edt = datetime.strptime(primary_dict["endDateTime"], "%Y-%m-%dT%H:%M:%S%z")

    json_dict["vmpJobId"] = primary_dict["occurrenceId"]
    # The organiser ID has extra AAA characters at the end compared to the spreadsheet submitted by HOHK
    if len(primary_dict["sponsoringOrganizationID"]) == 18:
        primary_dict["sponsoringOrganizationID"] = primary_dict[
            "sponsoringOrganizationID"
        ][:-3]
    json_dict["organiserId"] = primary_dict["sponsoringOrganizationID"]

    json_dict["visibility"] = "public"
    json_dict["isFull"] = (
        primary_dict["maximumAttendance"] - primary_dict["volunteersNeeded"]
    ) <= 0
    json_dict["publishedAt"] = primary_dict["voCreatedDate"].replace("Z", ".000Z")

    name = {}
    if has_english:
        name["en"] = json_dict_eng["title"]

    if has_chinese:
        name["zh"] = json_dict_chi["title"]

    json_dict["name"] = name

    description = {}
    if has_english:
        description["en"] = json_dict_eng.get(
            "description", "Please visit HandsOn Hong Kong to find out more."
        ).strip()

    if has_chinese:
        description["zh"] = json_dict_chi.get(
            "description", "請瀏覽到HandsOn Hong Kong 網站了解更多。"
        ).strip()

    json_dict["description"] = description

    json_dict["appImage"] = primary_dict["voThumbnailUrl"]  # Base64 image string 4:3
    json_dict["webImage"] = primary_dict["voThumbnailUrl"]  # supposed to be 16:9
    json_dict["url"] = primary_dict["detailUrl"]

    json_dict["applicationStart"] = primary_dict["ocCreatedDate"].replace("Z", ".000Z")
    json_dict["applicationEnd"] = (
        (edt + timedelta(days=-1)).strftime("%Y-%m-%dT%H:%M:%SZ").replace("Z", ".000Z")
    )
    json_dict["serviceStart"] = primary_dict["startDateTime"].replace("Z", ".000Z")
    json_dict["serviceEnd"] = primary_dict["endDateTime"].replace("Z", ".000Z")

    schedules = {}
    if has_english:
        schedules["en"] = ("\n").join(
            [
                "Volunteer Service",
                sdt.astimezone(pytz.timezone("Asia/Hong_Kong")).strftime(
                    "%a, %d %B %Y %I:%M%p"
                ),
                edt.astimezone(pytz.timezone("Asia/Hong_Kong")).strftime(
                    "%a, %d %B %Y %I:%M%p"
                ),
                json_dict_eng.get("locationAddress", ""),
            ]
        )

    if has_chinese:
        schedules["zh"] = ("\n").join(
            [
                "義工服務",
                sdt.astimezone(pytz.timezone("Asia/Hong_Kong")).strftime(
                    "%a, %d %B %Y %I:%M%p"
                ),
                edt.astimezone(pytz.timezone("Asia/Hong_Kong")).strftime(
                    "%a, %d %B %Y %I:%M%p"
                ),
                json_dict_chi.get("locationAddress", ""),
            ]
        )

    json_dict["schedules"] = schedules
    json_dict["quota"] = primary_dict["maximumAttendance"]

    json_dict["locations"] = mapLocation(
        json_dict_eng.get("locationAddress", "").strip() if has_english else "",
        json_dict_chi.get("locationAddress", "").strip() if has_chinese else "",
    )
    if "categoryTags" in primary_dict:
        json_dict["causes"] = mapCauses(primary_dict["categoryTags"])

    if "populationsServed" in primary_dict:
        json_dict["recipients"] = mapRecipients(primary_dict["populationsServed"])

    # if "Nlatitude" in primary_dict and "Nlongitude" in primary_dict:
    #    json_dict['additionalInfo'] = {
    #        'locationLatitude': primary_dict['Nlatitude'],
    #        'locationLongitude': primary_dict['Nlongitude']
    #    }

    return json_dict


# key is hohk side (categorytags), value (causes) is JC side
causes_mapping = {
    "Animal Welfare": "ANIMAL_WELFARE",
    "Arts & Culture": "ARTS_CULTURE",
    "Civic & Community": "COMMUNITY_DEVELOPMENT",
    "Maintenance and renovation": "COMMUNITY_DEVELOPMENT",
    "Disaster and emergency": "CRISIS_SUPPORT",
    "Diversity and inclusion": "DIVERSITY_INCLUSION",
    "Training and Empowerment": "EDUCATION",
    "Education": "EDUCATION",
    "Environmental Conservation": "ENVIRONMENT",
    "Health and well-being": "HEALTH_SPORTS",
    "Food Assistance": "POVERTY",
    "Awareness and sharing information": "OTHERS",
    "Support and assistance": "OTHERS",
    "Waste Reduction": "ENVIRONMENT",
    "Health & Wellness": "HEALTH_SPORTS",
    "Health and Wellness": "HEALTH_SPORTS",
    "Hygiene": "HEALTH_SPORTS",
    "Hunger & Homelessness": "POVERTY",
    "Education (new)": "EDUCATION",
    "Assistance and Support for Elderly": "ELDERLY",
}


def mapCauses(json_list):
    new_list = []
    for cause in json_list:
        if cause not in causes_mapping:
            logging.info(f"Cause '{cause}' has no mapping")
        else:
            new_list.append(causes_mapping[cause])
    return new_list


# key is hohk side (populationsServed), value (recipients) is JC side
recipients_mapping = {
    "Animals": "ANIMAL",
    "Children and youth": "CHILDREN_YOUTH",
    "Disadvantaged women": "WOMEN",
    "Domestic & migrant workers": "FOREIGN_WORKERS",
    "Elderly": "ELDERLY",
    "Environment": "ENVIRONMENT",
    "Ethnic minorities": "ETHNIC_MINORITY",
    "Families": "FAMILIES",
    "LGBTQ": "LGBT",
    "Low income households": "LOW_INCOME",
    "People experiencing homelessness": "LOW_INCOME",
    "People with health conditions": "PATIENTS",
    "People with mental health conditions": "MENTAL_HEALTH",
    "People with physical disabilities": "DISABLED",
    "People with special educational needs": "CHILDREN_YOUTH",
    "Refugees and asylum seekers": "REFUGEES_ASYLUM",
    "Adults": "GENERAL_PUBLIC",
    "Environmental education": "ENVIRONMENT",
    "Hunger & homelessness": "LOW_INCOME",
}


def mapRecipients(json_list):
    new_list = []
    for recipient in json_list:
        if recipient not in recipients_mapping:
            logging.info(f"Recipient '{recipient}' has no mapping")
        else:
            new_list.append(recipients_mapping[recipient])
    return new_list


location_mapping = {
    "Jordan Valley St. Joseph\u2019s Catholic Primary School, 80 Choi Ha Road, Kowloon Bay": "KWUN_TONG",
    "Tung Chung Catholic Primary School, 8 Yat Tung St, Yau Tung Estate, Tung Chung": "ISLANDS",
    "Nim Shue Wan Nim Shue Wan, Discovery Bay  Hong Kong": "ISLANDS",
    "Room 3B, 3/F, Splendid Centre, 100 Larch Street, Tai Kok Tsui, Kowloon": "YAU_TSIM_MONG",
    "outside Tsim Sha Tsui Marriage Registry, 10 Salisbury Rd, Tsim Sha Tsui": "YAU_TSIM_MONG",
    "Impact Hong Kong Guest Room, 29 Oak St, Tai Kok Tsui": "YAU_TSIM_MONG",
    "12/F, MONGKOK CHRISTIAN CENTRE, 56 BUTE STREET, MONGKOK KOWLOON  Hong Kong": "YAU_TSIM_MONG",
    "Unit B, 14/F, Koon Wo Industrial Building, 63-75 Ta Chuen Ping Street, Kwai Chung": "KWAI_TSING",
    "In-office Hong Kong Hong Kong": "HONG_KONG",
    "International Christian Life Centre, Flat B, 1/F, Ngun Hoi Mansion, 163 Hai Tan Street, Sham Shui Po": "SHAM_SHUI_PO",
    "Unit 2D, Worldwide Centre 123 Tung Chau Street, Tai Kok Tsui": "YAU_TSIM_MONG",
    "Kowloon & Hong Kong Island": "HONG_KONG",
    "Tseung Kwan O & Hang Hau": "SAI_KUNG",
    "4/F, 64 Tsun Yip Street, South Asia Commercial Centre, Kwun Tong": "KWUN_TONG",
    "Ground Floor, Un Lok House, Un Chau Estate, Shamshuipo, Kowloon, Hong Kong.": "SHAM_SHUI_PO",
    "51 Pitt Street, Mong Kok": "YAU_TSIM_MONG",
    "Kwun Tong Public Pier": "KWUN_TONG",
    "outside Tsim Sha Tsui Marriage Registry, 10 Salisbury Rd, Tsim Sha Tsui": "YAU_TSIM_MONG",
    "Shek Yam East Estate, Kwai Chung": "KWAI_TSING",
    "8/F, Two Exchange Square, 8 Connaught Place, Central, Hong Kong": "CENTRAL_AND_WESTERN",
    "Any recycling drop-off points": "HONG_KONG",
    "301,Tung Sing House, Lei Tung Estate": "SOUTHERN",
    "Lei Tung Estate near Lei Tung MTR exit B": "SOUTHERN",
    "Cheung Sha Wan": "SHAM_SHUI_PO",
    "Kwai Chung": "KWAI_TSING",
    "Sai Ying Pun": "CENTRAL_AND_WESTERN",
    "Tseung Kwan O": "SAI_KUNG",
    "Quarry Bay": "EASTERN",
    "Eaton Hong Kong, 380 Nathan Road, near hotel parking lot": "YAU_TSIM_MONG",
    "Tsim Sha Tsui area": "YAU_TSIM_MONG",
    "17/F, Block E, Chungking Mansions, 36-44 Nathan Road, Tsim Sha Tsui": "YAU_TSIM_MONG",
    "To be determined Kowloon Hong Kong Hong Kong": "HONG_KONG",
    "Aberdeen Sports Ground, 108 Wong Chuk Hang Road, Aberdeen": "SOUTHERN",
    "Virtual Volunteering": "ONLINE",
    "HHCKLA Buddhist Wong Cho Sum Primary School, 38 Po Lam Road N, King Lam Estate, Tseung Kwan O": "SAI_KUNG",
    "Shop 22, G/F, Hoi Lai Shopping Centre, Sham Shui Po": "SHAM_SHUI_PO",
    "Room 606, 6/F, 299QRC Nos, 287-299 Queen\u2019s Road Central, Central": "CENTRAL_AND_WESTERN",
    "Hong Kong Southern District": "SOUTHERN",
    "DIY Project": "HONG_KONG",
    "G/F, Fung Sing Building, 235 Hai Tan Street, Sham Shui Po, Kowloon": "SHAM_SHUI_PO",
    "HandsOn Hong Kong Office, Lai Chi Kok": "SHAM_SHUI_PO",
    "Farend of Butterfly Beach, Tuen Mun": "TUEN_MUN",
    "1/F, Car Park Building, Harmony Garden, Siu Sai Wan": "EASTERN",
    "Hong Kong Wetland Park, Wetland Park Road, Tin Shui Wai": "YUEN_LONG",
    "G/F, Tung Lam Court, Hing Tung Estate, Shau Kei Wan": "EASTERN",
    "Cheung Sha Wan or Wong Tai Sin": "HONG_KONG",
    "Tin Hau MTR station Exit A2": "WAN_CHAI",
    "Central Pier No.3": "CENTRAL_AND_WESTERN",
    "Morrison Hill Road Public Toilet": "WAN_CHAI",
    "Wanchai MTR station Exit B2": "WAN_CHAI",
    "Outside Fortress Hill MTR Exit A, in front of Wellcome supermarket": "EASTERN",
    "19/F Berkshire House, Taikoo": "EASTERN",
    "Bradbury Child Care Centre, 3/F, Holy Trinity Church Centenary Bradbury Building, 135 Ma Tau Chung Road, Ma Tau Wai": "KOWLOON_CITY",
    "Room 606, 6/F, 299QRC Nos, 287-299 Queen's Road Central, Central": "CENTRAL_AND_WESTERN",
    "Online via Zoom": "ONLINE",
    "Block A2, Yau Tong Industrial City, 17-25 Ko Fai Road,Yau Tong": "KWUN_TONG",
    "Chuan Kei Factory Building, 15-23 Kin Hong Street, Kwai Chung": "KWAI_TSING",
    "SAHK Jockey Club Elaine Field School (Boarding), 1 Fu Chung Lane, Tai Po, New Territories": "TAI_PO",
    "Hong Kong Golden Beach, So Kwun Wat (Tuen Mun)": "TUEN_MUN",
    "Phase 1, Long Ping Estate, Yuen Long, N.T Hong Kong Hong Kong": "YUEN_LONG",
    "Mei Wan Street, Tsuen Wan": "TSUEN_WAN",
    "Unit L & K, 1FL, WAI CHEUNG INDUSTRIAL CENTRE, 5 SHEK PAI TAU ROAD, Tuen Mun, HK": "TUEN_MUN",
}


# can actually have multiple districts, but just treat that as Hong Kong or Kowloon
def mapLocation(locationAddressEn, locationAddressZh):
    new_list = []
    if (
        locationAddressEn not in location_mapping
        and locationAddressZh not in location_mapping
    ):
        logging.info(
            f"Location '{locationAddressEn}' and '{locationAddressZh}' have no mapping"
        )
        new_list.append("HONG_KONG")
    elif locationAddressEn in location_mapping:
        new_list.append(location_mapping[locationAddressEn])
    elif locationAddressZh in location_mapping:
        new_list.append(location_mapping[locationAddressZh])

    return new_list
