
import requests
# from requests.auth import HTTPBasicAuth
import json
class DHIS2PersonData:
  def __init__(self, username, password, url):
    self.username = username
    self.password = password
    self.url = url

  async def get_client_details(self):
    response = requests.get(self.url + '/api/users.json', auth=(self.username,self.password), verify=False)
    if response.status_code == 200:
      return json.loads(response.content.decode('utf-8'))
    else:
      return None

  