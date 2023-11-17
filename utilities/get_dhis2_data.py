
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth
import json


headers = {
'Content-type': 'application/json'
}
class DHIS2Data:
  def __init__(self, username, password, url, data):
    self.username = username
    self.password = password
    self.url = url
    self.data = data

  async def get_departments_from_datastore(self):
    response = requests.get(self.url + '/api/dataStore.json', auth=(self.username,self.password), verify=False)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return None
  
  async def upload_metadata(self):
    path = 'metadata.json?importMode=COMMIT&dryRun=false&identifier=UID&'
    path += 'importReportMode=ERRORS&preheatMode=REFERENCE&importStrategy=CREATE_AND_UPDATE&atomicMode=ALL&'
    path += 'mergeMode=MERGE&flushMode=AUTO&skipSharing=false&skipValidation=false&inclusionStrategy=NON_NULL&format=json'
    response = requests.post(self.url + '/api/' + path, auth = HTTPBasicAuth(self.username,self.password), headers=headers, data=json.dumps(self.data))
    if response.status_code == 200:
      print("$$$$$$$$$$$$$$$$$@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
      print("$$$$$$$$$$$$$$$$$@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
      print("$$$$$$$$$$$$$$$$$@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
      print("$$$$$$$$$$$$$$$$$@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))

  #  WISN DATASET
  async def get_wisn_dataset(self):
    path = 'dataSets/ggoiwX3RSRr.json?fields=*,dataSetElements[id,dataElement[id],dataSet[id]],!href'
    response = requests.get(self.url + '/api/' + path, auth=(self.username,self.password), verify=False)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))
    
  async def update_wisn_dataset(self):
    path = 'metadata.json?importMode=COMMIT&dryRun=false&identifier=UID&importStrategy=CREATE_AND_UPDATE&mergeMode=MERGE'
    response = requests.post(self.url + '/api/' + path, auth = HTTPBasicAuth(self.username,self.password), headers=headers, data=json.dumps(self.data))
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))
  
  # END OF WISN DATASET

  #  HRH DATASET
  async def get_hrh_dataset(self):
    path = 'dataSets/MjO0xdyZSnO.json?fields=*,dataSetElements[dataElement[id],dataSet[id]],!href'
    response = requests.get(self.url + '/api/' + path, auth=(self.username,self.password), verify=False)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))
    
  async def update_hrh_dataset(self):
    path = 'metadata'
    response = requests.post(self.url + '/api/' + path, auth = HTTPBasicAuth(self.username,self.password), headers=headers, data=json.dumps(self.data))
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))
  
  # END OF HRH DATASET

  # HRH data element group
  async def get_hrh_dataelement_group(self):
    path = 'dataElementGroups/gIMeIniXGeP.json?fields=*,!href'
    response = requests.get(self.url + '/api/' + path, auth=(self.username,self.password), verify=False)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))
  
  async def update_hrh_datalement_group(self):
    # print("@#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    # print(json.dumps(self.data))
    # print("@#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    path = 'dataElementGroups/gIMeIniXGeP.json'
    response = requests.put(self.url + '/api/' + path, auth = HTTPBasicAuth(self.username,self.password), headers=headers, data=json.dumps(self.data))
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))
  # End of HRH data element group

  

  # HRH data element group
  async def get_hmis_dataelement_group(self):
    path = 'dataElementGroups/aVJOJ0kbcGd.json?fields=*,!href'
    response = requests.get(self.url + '/api/' + path, auth=(self.username,self.password), verify=False)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))
  
  async def update_hmis_datalement_group(self):
    path = 'dataElementGroups/aVJOJ0kbcGd.json'
    response = requests.put(self.url + '/api/' + path, auth = HTTPBasicAuth(self.username,self.password), headers=headers, data=json.dumps(self.data))
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))
  # End of HRH data element group

  # WISN Pages datastore
  async def get_wisn_data_store_page(self):
    path = 'sqlViews/NLYAMgnmTuI/data.json?paging=false&filter=namespacekey:ilike:wisnreport&filter=namespace:eq:wisn-poa-pages&cacheNone=' + str(datetime.now().timestamp())
    response = requests.get(self.url + '/api/' + path, auth=(self.username,self.password), verify=False)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))
  
  async def update_wisn_pages_datastore(self):
    path = 'dataStore/wisn-poa-pages'
    response = requests.put(self.url + '/api/' + path + "/"+ self.data['id'], auth = HTTPBasicAuth(self.username,self.password), headers=headers, data=json.dumps(self.data))
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))

  # End WISN pages datastore

  # Departments
  async def get_departments_from_datastore(self):
    path = 'sqlViews/NLYAMgnmTuI/data.json?paging=false&filter=namespace:eq:departments&cacheNone=' + str(datetime.now().timestamp())
    response = requests.get(self.url + '/api/' + path, auth=(self.username,self.password), verify=False)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))['listGrid']['rows']
    else:
      return json.loads(response.content.decode('utf-8'))

  async def update_department(self):
    path = 'dataStore/departments/' + self.data['id'] + '.json'
    response = requests.put(self.url + '/api/' + path, auth = HTTPBasicAuth(self.username,self.password), headers=headers, data=json.dumps(self.data))
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))
  
  async def create_department(self):
    print(self.data['id'])
    path = 'dataStore/departments/' + self.data['id'] + '.json'
    response = requests.post(self.url + '/api/' + path, auth = HTTPBasicAuth(self.username,self.password), headers=headers, data=json.dumps(self.data))
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))

  # End of departments

  # Cadres

  async def get_cadres_from_datastore(self):
    path = 'sqlViews/NLYAMgnmTuI/data.json?paging=false&filter=namespace:eq:cadres'
    response = requests.get(self.url + '/api/' + path, auth=(self.username,self.password), verify=False)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))['listGrid']['rows']
    else:
      return json.loads(response.content.decode('utf-8'))

  async def create_cadre(self):
    path = 'dataStore/cadres/' + self.data['id'] + '.json'
    response = requests.post(self.url + '/api/' + path, auth = HTTPBasicAuth(self.username,self.password), headers=headers, data=json.dumps(self.data))
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))
  
  async def update_cadre(self):
    path = 'dataStore/cadres/' + self.data['id'] + '.json'
    response = requests.put(self.url + '/api/' + path, auth = HTTPBasicAuth(self.username,self.password), headers=headers, data=json.dumps(self.data))
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))
  
  
  async def create_cadre_presets(self):
    path = 'dataStore/cadres-presets/' + self.data['id'] + '.json'
    response = requests.post(self.url + '/api/' + path, auth = HTTPBasicAuth(self.username,self.password), headers=headers, data=json.dumps(self.data))
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))

  # End of cadres

  async def get_system_ids(self):
    response = requests.get(self.url + '/api/system/id.json?limit=10', auth=(self.username,self.password), verify=False)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))['codes']
    else:
      return None

  async def get_client_details(self):
    response = requests.get(self.url + '/api/users.json', auth=(self.username,self.password), verify=False)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return None

  async def get_all_dataelements(self):
    response = requests.get(self.url + '/api/dataElements.json?fields=id,name,code&paging=false', auth=(self.username,self.password), verify=False)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))['dataElements']
    else:
      return None
  
  async def assign_dataelements_to_dataset_and_preset(self):
    path = 'metadata.json?importMode=COMMIT&dryRun=false&identifier=UID&importStrategy=CREATE_AND_UPDATE&mergeMode=MERGE'
    response = requests.post(self.url + '/api/' + path, auth = HTTPBasicAuth(self.username,self.password), headers=headers, data=json.dumps(self.data))
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))
