
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
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))

  #  WISN DATASET
  async def get_wisn_dataset(self):
    path = 'dataSets/ggoiwX3RSRr.json?fields=*,!href'
    response = requests.get(self.url + '/api/' + path, auth=(self.username,self.password), verify=False)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))
    
  async def update_wisn_dataset(self):
    path = 'dataSets/ggoiwX3RSRr.json?fields=*,!href'
    response = requests.post(self.url + '/api/' + path, auth = HTTPBasicAuth(self.username,self.password), headers=headers, data=json.dumps(self.data))
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))
  
  # END OF WISN DATASET

  #  HRH DATASET
  async def get_hrh_dataset(self):
    path = 'dataSets/MjO0xdyZSnO.json?fields=*,!href'
    response = requests.get(self.url + '/api/' + path, auth=(self.username,self.password), verify=False)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))
    
  async def update_hrh_dataset(self):
    path = 'dataSets/MjO0xdyZSnO.json?fields=*,!href'
    response = requests.post(self.url + '/api/' + path, auth = HTTPBasicAuth(self.username,self.password), headers=headers, data=json.dumps(self.data))
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return json.loads(response.content.decode('utf-8'))
  
  # END OF HRH DATASET


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

  