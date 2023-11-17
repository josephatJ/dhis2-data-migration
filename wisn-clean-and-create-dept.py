
# Josephat Mwakyusa & UDSM DHIS2 LAB, June 23 2021

import asyncio
import json
import os
# sudo pip3 install python-dotenv
from dotenv import dotenv_values
from requests.auth import HTTPBasicAuth
from utilities.generate_ids import get_random_string
from utilities.get_data_from_csv import GetDataFromCSVFile
from utilities.get_dhis2_data  import DHIS2Data
from utilities.sanitize_data import clean_metadata_from_errors, format_cadre_for_datastore, format_cadre_presets, formulate_current_dpt_configs, formulate_data_elements_from_department, formulate_datastore_payload, is_department_already_on_datastore, merge_departments, remove_duplicate_cadres
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
            cadre_type = ""
            cadre_group = ""
            if item_row[5] is not None:
                cadre_type = item_row[5]
            if item_row[4] is not None:
                cadre_group = item_row[4]
            cadres.append({"name":item_row[2], "code": item_row[3], "type": cadre_type, "group": cadre_group })
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
            cadre_type = ""
            cadre_group = ""
            if item_row[5] is not None:
                cadre_type = item_row[5]
            if item_row[4] is not None:
                cadre_group = item_row[4]
            cadre = {
                "name": item_row[2],
                "code": item_row[3],
                "type": cadre_type,
                "group": cadre_group
            }
            cadres = []
            cadres.append(cadre)
            dpts[item_row[0]]= {
                "name": item_row[0],
                "code": item_row[1],
                "cadres": cadres
            }
        else:
            cadre_type = ""
            cadre_group = ""
            if item_row[5] is not None:
                cadre_type = item_row[5]
            if item_row[4] is not None:
                cadre_group = item_row[4]
            cadre = {
                "name": item_row[2],
                "code": item_row[3],
                "type": cadre_type,
                "group": cadre_group

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
    current_date = datetime.now().timestamp()
    # print(formulate_department_payload("1","2","code", [], current_date))
    # print(dpt_data)

    # Update & Create cadres accordinly
    all_cadres_from_excel = []
    # keyed_existing_cadres = {}
    # for dpt_data_row in dpt_data:
    #     cadre_details = format_cadre_for_datastore(dpt_data_row)
    #     all_cadres_from_excel.append(cadre_details)
    
    # # Get stored cadres

    # stored_cadres_payload = await dhis2Data.get_cadres_from_datastore()
    # if stored_cadres_payload is not None:
    #     keyed_existing_cadres = formulate_datastore_payload(stored_cadres_payload)
    #     path =  os.getcwd() + "/metadata/metadata.json"
    #     with open(path, "w") as jsonfile:
    #         jsonfile.write(json.dumps(keyed_existing_cadres))
        
    #     for cadre_metadata in all_cadres_from_excel:
    #         if cadre_metadata['cadreBasicInfo']['name'] not in keyed_existing_cadres:
    #             print("CADRE `" + cadre_metadata['cadreBasicInfo']['name'] + "` DOES NOT EXIST")
    #             # print(json.dumps(cadre_metadata['cadreBasicInfo']))

    #             dhis2Data = DHIS2Data(username,password,url,cadre_metadata['cadreBasicInfo'] )
    #             cadre_create_response = await dhis2Data.create_cadre()
    #             keyed_existing_cadres[cadre_metadata['cadreBasicInfo']['name']] = cadre_metadata['cadreBasicInfo']
    #             print("cadre_create_response", cadre_create_response)

    #             # Create cadre presets
    #             # cadre_presets = format_cadre_presets(cadre_metadata)
    #             # dhis2Data = DHIS2Data(username,password,url,cadre_presets)
    #             # cadre_presets_create_response = await dhis2Data.create_cadre_presets()
    #             # print("cadre_presets_create_response", cadre_presets_create_response)
    #         else:
    #             print("***CADRE `" + cadre_metadata['cadreBasicInfo']['name'] + "-" + cadre_metadata['cadreBasicInfo']['id'] + "` EXISTS, DO NOTHING")
    #             # Update update_cadre
    #             cadre_info = keyed_existing_cadres[cadre_metadata['cadreBasicInfo']['name']]
    #             cadre_info['type'] = cadre_metadata['cadreBasicInfo']['type']
    #             print("cadre_info", json.dumps(cadre_info))
    #             dhis2Data = DHIS2Data(username,password,url,cadre_info)
    #             cadre_update_response = await dhis2Data.update_cadre()
    #             # print(json.dumps(keyed_existing_cadres[cadre_metadata['cadreBasicInfo']['name']]))



    # Get data elements
    dhis2Data = DHIS2Data(username,password,url, {})
    available_elems = await dhis2Data.get_all_dataelements()
    elems_to_upate = []
    for available_elem in available_elems:
        if 'staff' not in available_elem['name'] or '.' not in available_elem['name']:
            print(available_elem['name'])
            elems_to_upate.append({
                "dataElement": {
                    "id": available_elem['id']
                },
                "dataSet": {
                    "id": "ggoiwX3RSRr"
                }
            })
    
    dataset_details = await dhis2Data.get_wisn_dataset()
    if dataset_details is not None:
        dataset_payload = dataset_details
        merged_elems = [*dataset_payload['dataSetElements'], *elems_to_upate]
        dataset_elements = []
        cleaned = []
        check = {}
        for merged_elem in merged_elems:
            if merged_elem['dataElement']['id'] not in check:
                cleaned.append(merged_elem)
                check[merged_elem['dataElement']['id']] = merged_elem['dataElement']['id']

        dataset_payload['dataSetElements'] = cleaned
        path =  os.getcwd() + "/metadata/dataset.json"
        with open(path, "w") as jsonfile:
            jsonfile.write(json.dumps({"dataSets": [dataset_payload]}))
        dhis2Data = DHIS2Data(username,password,url, {"dataSets": [dataset_payload]})
        update_response = await dhis2Data.update_wisn_dataset()

        print("update_response_WISN", update_response)


    # Existing staffs
    # dhis2Data = DHIS2Data(username,password,url, {})
    # available_elems = await dhis2Data.get_all_dataelements()
    # elems_to_upate = []
    # for available_elem in available_elems:
    #     if '.' in available_elem['name']:
    #         elems_to_upate.append({
    #             "dataElement": {
    #                 "id": available_elem['id']
    #             },
    #             "dataSet": {
    #                 "id": "MjO0xdyZSnO"
    #             }
    #         })
    
    # dataset_details = await dhis2Data.get_hrh_dataset()
    # if dataset_details is not None:
    #     dataset_payload = dataset_details
    #     merged_elems = [*dataset_payload['dataSetElements'], *elems_to_upate]
    #     dataset_elements = []
    #     cleaned = []
    #     check = {}
    #     for merged_elem in merged_elems:
    #         if merged_elem['dataElement']['id'] not in check:
    #             cleaned.append(merged_elem)
    #             check[merged_elem['dataElement']['id']] = merged_elem['dataElement']['id']

    #     dataset_payload['dataSetElements'] = cleaned
    #     dhis2Data = DHIS2Data(username,password,url, {"dataSets": [dataset_payload]})
    #     update_response = await dhis2Data.update_wisn_dataset()
    #     print(update_response)


    # dhis2Data = DHIS2Data(username,password,url, {})
    # available_elems = await dhis2Data.get_all_dataelements()
    # elems_to_upate = []
    # for available_elem in available_elems:
    #     if '.' in available_elem['name']:
    #         elems_to_upate.append({
    #             "dataElement": {
    #                 "id": available_elem['id']
    #             },
    #             "dataSet": {
    #                 "id": "MjO0xdyZSnO"
    #             }
    #         })
    
    dataset_details = await dhis2Data.get_hrh_dataelement_group()
    if dataset_details is not None:
        dataset_payload = dataset_details
        merged_elems = [*dataset_payload['dataSetElements'], *elems_to_upate]
        dataset_elements = []
        cleaned = []
        check = {}
        for merged_elem in merged_elems:
            if merged_elem['dataElement']['id'] not in check:
                cleaned.append(merged_elem)
                check[merged_elem['dataElement']['id']] = merged_elem['dataElement']['id']

        dataset_payload['dataSetElements'] = cleaned
        dhis2Data = DHIS2Data(username,password,url, {"dataSets": [dataset_payload]})
        update_response = await dhis2Data.update_wisn_dataset()
        print(update_response)


    stored_departments = await dhis2Data.get_departments_from_datastore()
    # print(stored_departments)
    if stored_departments is not None:
        formatted_dept_payload = formulate_datastore_payload(stored_departments)
        dpts_only = get_department_only(dpt_data)
        dpt_keys = dpts_only.keys()

        count = 0
        for key in dpt_keys:
            department = dpts_only[key]
            ids1 = []
            idsawt = []
            idsspt = []
            timeids = []
            presetids = []
            hrhids = []
            suppyids = []
            indids = []
            metadata = await formulate_data_elements_from_department(dpts_only[key], ids1, idsawt,idsspt,timeids, presetids, hrhids, suppyids,indids)
            # print(json.dumps(metadata))

            # TODO: Add support to create departments
            dpts_ids =await dhis2Data.get_system_ids()
            if dpts_ids is not None:
                all_dpt_cadre_codes = []
                for cadre in department['cadres']:
                    all_dpt_cadre_codes.append(cadre['code'])
                # namespace ='departments'
                department_payload = {}
                if department['name'] in formatted_dept_payload:
                    department_payload = formatted_dept_payload[department['name']]
                    cadres = [*formatted_dept_payload[department['name']], *all_dpt_cadre_codes]
                    check = {}
                    new_cadres = []
                    for cadre in cadres:
                        if cadre not in check:
                            check[cadre] = cadre
                            new_cadres.append(cadre)
                    department_payload['cadres'] = new_cadres
                    department_payload['cadresHistory'] = new_cadres
                    dhis2Data = DHIS2Data(username,password,url, department_payload)
                    res = await dhis2Data.update_department()
                    # Update department accordingly

                else:
                    department_payload = {
                        "id": get_random_string(11),
                        "name": department['name'],
                        "code": department['code'],
                        "group": "",
                        "cadres": all_dpt_cadre_codes,
                        "cadresHistory": all_dpt_cadre_codes,
                        "created": current_date,
                        "lastUpdated": current_date,
                    }
                    print("department_payload", department_payload)
                    count = count + 1
                    dhis2Data = DHIS2Data(username,password,url, department_payload)
                    res = await dhis2Data.create_department()

            dhis2Data = DHIS2Data(username,password,url, metadata)
            metadata_response = await dhis2Data.upload_metadata()
            path =  os.getcwd() + "/metadata/elems_and_inds.json"
            with open(path, "w") as jsonfile:
                jsonfile.write(json.dumps(metadata))
            path =  os.getcwd() + "/metadata/conflicts.json"
            with open(path, "w") as jsonfile:
                jsonfile.write(json.dumps(metadata_response))

            metadata = clean_metadata_from_errors(metadata_response, metadata)

            dhis2Data = DHIS2Data(username,password,url, metadata)
            metadata_response_after_correction = await dhis2Data.upload_metadata()
            path =  os.getcwd() + "/metadata/metadata.json"
            with open(path, "w") as jsonfile:
                jsonfile.write(json.dumps(metadata))
            path =  os.getcwd() + "/metadata/conflicts_after.json"
            with open(path, "w") as jsonfile:
                jsonfile.write(json.dumps(metadata_response_after_correction))
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
                    merged_elems = [*dataset_payload['dataSetElements'], *dataset_elements]
                    check = {}
                    uniq_elems = []
                    for merged_elem in merged_elems:
                        if merged_elem['dataElement']['id'] not in check:
                            uniq_elems.append(merged_elem)
                            check[merged_elem['dataElement']['id']] = merged_elem['dataElement']['id']
                    dataset_payload['dataSetElements'] = uniq_elems
                    path =  os.getcwd() + "/metadata/dataset.json"
                    with open(path, "w") as jsonfile:
                        jsonfile.write(json.dumps(dataset_details))
                    dhis2Data = DHIS2Data(username,password,url, {"dataSets": [dataset_payload]})
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
                            dataset_elements.append(dataset_element)
                    merged_elems = [*dataset_payload['dataSetElements'], *dataset_elements]
                    check = {}
                    uniq_elems = []
                    for merged_elem in merged_elems:
                        if merged_elem['dataElement']['id'] not in check:
                            uniq_elems.append(merged_elem)
                            check[merged_elem['dataElement']['id']] = merged_elem['dataElement']['id']
                    dataset_payload['dataSetElements'] = uniq_elems
                    dhis2Data = DHIS2Data(username,password,url,  {"dataSets": [dataset_payload]})
                    update_response = await dhis2Data.update_hrh_dataset()

                    print("update_response_HRH", update_response)

                # update HRH data element group
                # 1. Get data element group
                # 2. Update
                group_details = await dhis2Data.get_hrh_dataelement_group()
                if group_details is not None:
                    payload = group_details
                    elems = []
                    for data_element in metadata['dataElements']:
                        if "." in data_element['name']:
                            elems.append({"id": data_element['id']})
                    
                    check = {}
                    uniq_elems = []
                    for data_element in elems:
                        if data_element['id'] not in check:
                            uniq_elems.append(data_element)
                            check[data_element['id']] = data_element
                    payload['dataElements'] = uniq_elems
                    path =  os.getcwd() + "/metadata/element_group.json"
                    with open(path, "w") as jsonfile:
                        jsonfile.write(json.dumps(payload))
                    dhis2Data = DHIS2Data(username,password,url, payload)
                    update_response = await dhis2Data.update_hrh_datalement_group()

                    print("update_response_HRH_dataelement_group", update_response)

                # update HMIS data element group
                # 1. Get data element group
                # 2. Update
                group_details = await dhis2Data.get_hmis_dataelement_group()
                if group_details is not None:
                    payload = group_details
                    elems = []
                    for data_element in metadata['dataElements']:
                        if "HMIS_#" in data_element['name']:
                            elems.append({"id": data_element['id']})
                    check = {}
                    uniq_elems = []
                    for data_element in elems:
                        if data_element['id'] not in check:
                            uniq_elems.append(data_element)
                            check[data_element['id']] = data_element
                    payload['dataElements']=  uniq_elems
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