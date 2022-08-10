
# Josephat Mwakyusa & UDSM DHIS2 LAB, June 23 2021

import asyncio
import json
import os
# sudo pip3 install python-dotenv
from dotenv import dotenv_values
from requests.auth import HTTPBasicAuth
from utilities.get_data_from_csv import GetDataFromCSVFile
from utilities.get_dhis2_data  import DHIS2Data
from utilities.sanitize_data import formulate_data_elements_from_department
from datetime import datetime

headers = {
'Content-type': 'application/json'
}


def formulate_department_payload(id,name,code,cadres_codes,current_date):
    dpt_payload = {
        "id": id,
        "name": name,
        "code": code,
        "group": '',
        "cadres": cadres_codes,
        "cadresHistory": cadres_codes,
        "created": current_date,
        "lastUpdated": current_date,
      }
    return dpt_payload


def get_department_only(data):
    dpts = {}
    department_check = {}
    for item_row in data:
        # print(index)
        if item_row[0] not in dpts:
            # print(item_row)
            department_check[item_row[0]] = item_row[1]
            dept_data = []
            dept_data.append(item_row[0])
            dept_data.append(item_row[1])
            cadres = []
            cadres.append({"name":item_row[2], "code": item_row[3] })
            dpts[item_row[0]]= {
                "name": item_row[0],
                "code": item_row[1],
                "cadres": cadres
            }
        elif len(dpts) == 0:
            department_check[item_row[0]] = item_row[1]
            dept_data = []
            dept_data.append(item_row[0])
            dept_data.append(item_row[1])
            cadre = {
                "name": item_row[2],
                "code": item_row[3]
            }
            cadres = []
            cadres.append(cadre)
            dpts[item_row[0]]= {
                "name": item_row[0],
                "code": item_row[1],
                "cadres": cadres
            }
        else:
            cadre = {
                "name": item_row[2],
                "code": item_row[3]
            }
            dpts[item_row[0]]['cadres'].append(cadre)
    return dpts

async def main():
    configs = dotenv_values(".env")
    username = configs.get('USERNAME')
    password = configs.get('PASSWORD')
    url = configs.get('URL')

    path = os.getcwd() + "/metadata/departments.csv"

    dataFromExcel = GetDataFromCSVFile(path)
    dpt_data = await dataFromExcel.get_departments()

    dhis2Data = DHIS2Data(username,password,url, {})
    # system_id = await dhis2Data.get_system_ids()
    # print(system_id)
    current_date = datetime.now()
    # print(formulate_department_payload("1","2","code", [], current_date))
    # print(dpt_data)

    dpts_only = get_department_only(dpt_data)
    dpt_keys = dpts_only.keys()

    ids1 = await dhis2Data.get_system_ids()
    idsawt = await dhis2Data.get_system_ids()
    idsspt = await dhis2Data.get_system_ids()
    timeids = await dhis2Data.get_system_ids()
    presetids = await dhis2Data.get_system_ids()
    hrhids = await dhis2Data.get_system_ids()
    suppyids = await dhis2Data.get_system_ids()
    indids = await dhis2Data.get_system_ids()

    for key in dpt_keys:
        ids1 = await dhis2Data.get_system_ids()
        idsawt = await dhis2Data.get_system_ids()
        idsspt = await dhis2Data.get_system_ids()
        timeids = await dhis2Data.get_system_ids()
        presetids = await dhis2Data.get_system_ids()
        hrhids = await dhis2Data.get_system_ids()
        suppyids = await dhis2Data.get_system_ids()
        indids = await dhis2Data.get_system_ids()
        metadata = await formulate_data_elements_from_department(dpts_only[key], ids1, idsawt,idsspt,timeids, presetids, hrhids, suppyids,indids)
        # print(json.dumps(metadata))

        dhis2Data = DHIS2Data(username,password,url, metadata)
        response = await dhis2Data.upload_metadata()

        # Update wisn dataset
        # 1. Get dataset
        # 2. Update
        dataset_details = await dhis2Data.get_wisn_dataset()
        if dataset_details is not None:
            dataset_payload = dataset_details
            dataset_elements = []
            for data_element in metadata['dataElements']:
                if data_element['name'].index(".") != -1 and data_element['name'].index(".") != -1:
                    dataset_element = {
                        "dataElement": {
                                "id": data_element['id'],
                            },
                        "dataSet": {
                                "id": 'ggoiwX3RSRr',
                            }
                    }
                    dataset_elements.append(dataset_element)
            dataset_payload['dataSetElements'].append(dataset_elements)
            dhis2Data = DHIS2Data(username,password,url, dataset_payload)
            update_response = await dhis2Data.update_wisn_dataset()

            print("update_response_WISN", update_response)
        
        # update HRH dataset
        # 1. Get dataset
        # 2. Update
        dataset_details = await dhis2Data.get_hrh_dataset()
        if dataset_details is not None:
            dataset_payload = dataset_details
            dataset_elements = []
            for data_element in metadata['dataElements']:
                if data_element['name'].index(".") != -1 and data_element['name'].index(".") != -1:
                    dataset_element = {
                        "dataElement": {
                                "id": data_element['id'],
                            },
                        "dataSet": {
                                "id": 'MjO0xdyZSnO',
                            }
                    }
                    dataset_elements.append(dataset_element)
            dataset_payload['dataSetElements'].append(dataset_elements)
            dhis2Data = DHIS2Data(username,password,url, dataset_payload)
            update_response = await dhis2Data.update_hrh_dataset()

            print("update_response_HRH", update_response)

        # update HRH data element group
        # 1. Get data element group
        # 2. Update
        dataset_details = await dhis2Data.get_hrh_dataset()
        if dataset_details is not None:
            dataset_payload = dataset_details
            dataset_elements = []
            for data_element in metadata['dataElements']:
                if data_element['name'].index(".") != -1 and data_element['name'].index(".") != -1:
                    dataset_element = {
                        "dataElement": {
                                "id": data_element['id'],
                            },
                        "dataSet": {
                                "id": 'MjO0xdyZSnO',
                            }
                    }
                    dataset_elements.append(dataset_element)
            dataset_payload['dataSetElements'].append(dataset_elements)
            dhis2Data = DHIS2Data(username,password,url, dataset_payload)
            update_response = await dhis2Data.update_hrh_dataset()

            print("update_response_HRH", update_response)

asyncio.run(main())