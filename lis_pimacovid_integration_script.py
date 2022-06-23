
# Josephat Mwakyusa, August 06 2021

import asyncio
import json
from requests.auth import HTTPBasicAuth
from utilities.get_dhis2_data  import DHIS2PersonData

headers = {
'Content-type': 'application/json'
}


async def main():
    username = 'lispimacovidintegration'
    password = 'Dhis@2022'
    url = 'https://covid19-admin.moh.go.tz'
    dhis2PersonData = DHIS2PersonData(username,password,url)
    dhis2PersonData.get_client_details()

asyncio.run(main())