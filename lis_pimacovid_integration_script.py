
# Josephat Mwakyusa & UDSM DHIS2 LAB, June 23 2021

import asyncio
import json
import os
# sudo pip3 install python-dotenv
from dotenv import dotenv_values
from requests.auth import HTTPBasicAuth
from utilities.get_dhis2_data  import DHIS2PersonData

headers = {
'Content-type': 'application/json'
}



async def main():
    configs = dotenv_values(".env")
    username = configs.get('USERNAME')
    password = configs.get('PASSWORD')
    url = configs.get('URL')
    dhis2PersonData = DHIS2PersonData(username,password,url)
    dhis2PersonData.get_client_details()

asyncio.run(main())