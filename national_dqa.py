
# Josephat Mwakyusa & UDSM DHIS2 LAB, Nov 13 2023

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
import requests
from requests.auth import HTTPBasicAuth

headers = {
'Content-type': 'application/json'
}

async def get_national_dqa(base_url,username,password,ou):
    url = base_url + '/api/40/tracker/trackedEntities.json?orgUnit=' + ou['id'] + '&ouMode=SELECTED&program=aAwyEeJysgl&paging=false'
    # print(url)
    response = requests.get(url, auth=(username,password), verify=False)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return None
async def update_tracked_entity_instance(base_url,username,password,trackedEntity,data):
    print(data)
    url = base_url + '/api/trackedEntityInstances/' + trackedEntity + '?program=aAwyEeJysgl'
    response = requests.put( url, auth = HTTPBasicAuth(username,password), headers=headers, data=json.dumps(data))
    print(response)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8')) 
# 
async def get_ous_by_level(base_url,username,password, level):
    response = requests.get(base_url + '/api/organisationUnits.json?fields=id,name,code,level&paging=false&filter=level:eq:' + str(level), auth=(username,password), verify=False)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return None

async def generate_id(base_url,username,password):
#    trackedEntityAttributes/${caseIdAttribute}/generate.json
    response = requests.get(base_url + '/api/trackedEntityAttributes/IXjYDRerA5C/generate.json', auth=(username,password), verify=False)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return None

async def main():
    configs = dotenv_values(".env")
    username = configs.get('USERNAME')
    password = configs.get('PASSWORD')
    base_url = configs.get('URL')
    print(username)
    print(password)
    print(base_url)
    count = 0
    levels = [4]
    for level in levels:
        ous_response = await get_ous_by_level(base_url, username, password,level)
        for ou in ous_response['organisationUnits']:
            # print(ou)
            response = await get_national_dqa(base_url, username, password,ou)
            # print(response)
            for instance in response['instances']:
                if 'attributes' in instance:
                    count = count + 1
                    attribues = []
                    for attribute in instance['attributes']:
                       attribues.append({
                            'attribute': attribute['attribute'],
                            'value': attribute['value']
                       })
                    # print(attribues)
                    reporting_level = 'HF'
                    if ou['level'] == 2:
                        reporting_level = 'Region'
                    elif ou['level'] == 3:
                        reporting_level = 'District'
                    else:
                        reporting_level = 'HF'
                    attribues.append({
                    'attribute': 'pqwKgj4l5kc',
                    'value': reporting_level
                    })
                    generated_id_response = await generate_id(base_url,username,password)
                    generate_seq_id = generated_id_response['value']
                    attribues.append({
                        'attribute': 'IXjYDRerA5C',
                        'value': generate_seq_id
                    })
                    data = {
                                "created": instance['createdAt'],
                                "orgUnit": instance['orgUnit'],
                                "createdAtClient": instance['createdAtClient'],
                                "trackedEntityInstance": instance['trackedEntity'],
                                "trackedEntityType": "hJgtfFgxk6Q",
                                "programOwners": [
                                    {
                                    "ownerOrgUnit": instance['orgUnit'],
                                    "program": "aAwyEeJysgl",
                                    "trackedEntityInstance": instance['trackedEntity']
                                    }
                                ],
                                "relationships": [],
                                "attributes": attribues
                            }
                    # print(json.dumps(data))
                    print("########################################")
                    print(count)
                    print(instance['trackedEntity'])
                    print(ou['name'])
                    print(ou['level'])
                    print(json.dumps(attribues))
                    print("########################################")
                    
                    update_response = await update_tracked_entity_instance(base_url, username, password, instance['trackedEntity'], data)
                    print(update_response)

asyncio.run(main())