
# Josephat Mwakyusa & UDSM DHIS2 LAB, June 23 2021

import asyncio
import json
import os
# sudo pip3 install python-dotenv
from dotenv import dotenv_values
from requests.auth import HTTPBasicAuth
from utilities.get_data_from_csv import GetDataFromCSVFile
from utilities.get_dhis2_data  import DHIS2Data
from utilities.sanitize_data import clean_metadata_from_errors, formulate_current_dpt_configs, formulate_data_elements_from_department, is_department_already_on_datastore, merge_departments, remove_duplicate_cadres
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
        department = dpts_only[key]
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

        # TODO: Add support to create departments
        dhis2Data = DHIS2Data(username,password,url, metadata)
        metadata_response = await dhis2Data.upload_metadata()
        # path =  os.getcwd() + "/metadata/data_reference.json"
        # with open(path, "w") as jsonfile:
        #     jsonfile.write(json.dumps(metadata_response))

        metadata = clean_metadata_from_errors(metadata_response, metadata)

        dhis2Data = DHIS2Data(username,password,url, metadata)
        metadata_response = await dhis2Data.upload_metadata()
        path =  os.getcwd() + "/metadata/metadata.json"
        with open(path, "w") as jsonfile:
            jsonfile.write(json.dumps(metadata))
        # Update wisn dataset
        # 1. Get dataset
        # 2. Update
        if metadata_response is not None:
            dataset_details = await dhis2Data.get_wisn_dataset()
            if dataset_details is not None:
                dataset_payload = dataset_details
                dataset_elements = []
                for data_element in metadata['dataElements']:
                    if "." not in data_element['name'] and "staff" not in data_element['name']:
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
                    if "." in data_element['name']:
                        dataset_element = {
                            "dataElement": {
                                    "id": data_element['id'],
                                },
                            "dataSet": {
                                    "id": 'MjO0xdyZSnO',
                                }
                        }
                        dataset_payload['dataSetElements'].append(dataset_element)
                dhis2Data = DHIS2Data(username,password,url, dataset_payload)
                update_response = await dhis2Data.update_hrh_dataset()

                print("update_response_HRH", update_response)

            # update HRH data element group
            # 1. Get data element group
            # 2. Update
            group_details = await dhis2Data.get_hrh_dataelement_group()
            if group_details is not None:
                payload = {
                    "dataElements": group_details['dataElements']
                }
                for data_element in metadata['dataElements']:
                    if "." in data_element['name']:
                        payload['dataElements'].append({"id": data_element['id']})
                dhis2Data = DHIS2Data(username,password,url, payload)
                update_response = await dhis2Data.update_hrh_datalement_group()

                print("update_response_HRH_dataelement_group", update_response)

            # update HMIS data element group
            # 1. Get data element group
            # 2. Update
            group_details = await dhis2Data.get_hmis_dataelement_group()
            if group_details is not None:
                payload = {
                    "dataElements": group_details['dataElements']
                }
                for data_element in metadata['dataElements']:
                    if "HMIS_#" in data_element['name']:
                        payload['dataElements'].append({"id": data_element['id']})
                dhis2Data = DHIS2Data(username,password,url, payload)
                update_response = await dhis2Data.update_hmis_datalement_group()

                print("update_response_HRH_dataelement_group", update_response)

            # update datastore
            # 1. Get datastore details
            # 2. Update
            current_dpt_configs = formulate_current_dpt_configs(department, department['cadres'], metadata)
            datastore_data = []
            datastore_details = await dhis2Data.get_wisn_data_store_page()
            if datastore_details is not None:
                analytics = datastore_details['listGrid']
                index = 2
                for row in analytics['rows']:
                    if 'value' in row[index]:
                        json_str = row[index]['value']
                        datastore_data.append(json.loads(json_str))
            
            
            if len(datastore_data) > 0:
                wisn_report = datastore_data[0]

                # 1 Check if the current department is on the departments within wisnreport payload
                exist = is_department_already_on_datastore(wisn_report['departments'], department)
                if exist == True:
                    print("EXIST, UPDATE RESPECTIVE DPT")
                    existing_dpt = {}
                    for dpt in wisn_report['departments']:
                        if 'id' in dpt:
                            if dpt['id'] == department['code']:
                                existing_dpt = dpt
                    existing_dpt_cadres = []
                    if 'cadres' in existing_dpt:
                        existing_dpt_cadres =  existing_dpt['cadres']
                    all_cadres = [*existing_dpt_cadres, *current_dpt_configs['cadres']]
                    filtered_cadres = []
                    for cadre in all_cadres:
                        if len(cadre['dataSource']) > 1 and 'id' in cadre['dataSource'][1]:
                            filtered_cadres.append(cadre)
                    all_non_duplicate_cadres =  remove_duplicate_cadres(filtered_cadres)
                    existing_dpt['cadres'] = all_non_duplicate_cadres
                    wisn_report['departments'] = merge_departments(wisn_report['departments'], existing_dpt)
                    path =  os.getcwd() + "/metadata/data_reference.json"
                    with open(path, "w") as jsonfile:
                        jsonfile.write(json.dumps(wisn_report))
                    
                    dhis2Data = DHIS2Data(username,password,url, wisn_report)
                    res = await dhis2Data.update_wisn_pages_datastore()

                else:
                    print("DOES NOT EXIST, ADD")
                    dpts = wisn_report['departments']
                    dpts = [*dpts,current_dpt_configs]
                    wisn_report['departments'] = dpts
                    path =  os.getcwd() + "/metadata/data_reference.json"
                    with open(path, "w") as jsonfile:
                        jsonfile.write(json.dumps(wisn_report))
                    dhis2Data = DHIS2Data(username,password,url, wisn_report)
                    res = await dhis2Data.update_wisn_pages_datastore()
                

asyncio.run(main())