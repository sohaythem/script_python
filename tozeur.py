#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
from requests.auth import AuthBase
from Crypto.Hash import HMAC
from Crypto.Hash import SHA256
from datetime import datetime, timezone
from dateutil.tz import tzlocal
import pandas
import json
import time


# Class to perform HMAC encoding
class AuthHmacMetosGet(AuthBase):
    # Creates HMAC authorization header for Metos REST service GET request.
    def __init__(self, apiRoute, publicKey, privateKey):
        self._publicKey = publicKey
        self._privateKey = privateKey
        self._method = 'GET'
        self._apiRoute = apiRoute

    def __call__(self, request):
        dateStamp = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        print("timestamp: ", dateStamp)
        request.headers['Date'] = dateStamp
        msg = (self._method + self._apiRoute + dateStamp + self._publicKey).encode(encoding='utf-8')
        h = HMAC.new(self._privateKey.encode(encoding='utf-8'), msg, SHA256)
        signature = h.hexdigest()
        request.headers['Authorization'] = 'hmac ' + self._publicKey + ':' + signature
        return request


# Endpoint of the API, version for example: v1
apiURI = 'https://api.fieldclimate.com/v1'

# HMAC Authentication credentials
publicKey = '*******************************'
privateKey = '******************************'


#mis à jour de jeux des données sur agridata
API_ENDPOINT3 = "http://www.agridata.tn/fr/api/3/action/resource_update"

header3 = {"authorization": "***********************"}


json3 = {
   
  "id": "4d73b77d-5dff-4e58-bd41-cfebfa7ae7b6",
  "description": "T : Température Air HC [°C] PR : Point de rosée [°C] P : Précipitation en mm HR : Humidité Relative HC [%] DV: Direction du vent [deg] VV : Vitesse du vent [m/s] RS : Rayonnement solaire [W/m2] DPV : Déficit de Pression de vapeur [mbar] HF : Humidité des Feuilles  [min] DT : DeltaT [°C] "  ,
  "description_ar":"معطيات مناخية",
  "name_ar": "معطيات مناخية",
    "name": "données climatiques",
	"format": "API"
}
r3 = requests.post(url = API_ENDPOINT3,headers = header3 , json = json3)

pastebin_url3 = r3.text 
print(r3.text )





# Obtention de date de dernier enregistrement
API_ENDPOINT1 = "http://www.agridata.tn/fr/api/3/action/datastore_search_sql?sql=SELECT date from \"4d73b77d-5dff-4e58-bd41-cfebfa7ae7b6\" order by date desc limit 1"

header = {"authorization": "****************************"}

 # sending post request and saving response as response object 
r = requests.get(url = API_ENDPOINT1,headers = header) 
COL1 = r.json()['result']['records']
print(COL1)
df = pandas.DataFrame(COL1)
last_date=df.date[0]
print(last_date)
date_time_obj = datetime.strptime(last_date, '%Y-%m-%d %H:%M:%S')
date_from=time.mktime(date_time_obj.timetuple())+4500



from_date=int(date_from)
print (str(from_date))
apiRoute = '/data/002043D6/raw/from/'+str(from_date)



# Service/Route that you wish to call
#apiRoute = '/data/00203DDE/hourly/from/'+str(int(posix1))+'/to/'+str(int(posix2))
#apiRoute = '/data/00203DDE/hourly/from/'
#apiRoute = '/data/00203DDE/raw/last/10000'
#apiRoute = '/data/002043D6/raw/from/1567036800/to/1574985600'
#apiRoute = '/user/stations'

auth = AuthHmacMetosGet(apiRoute, publicKey, privateKey)
response = requests.get(apiURI+apiRoute, headers={'Accept': 'application/json'}, auth=auth)
#print(response.json()['data'])

  # defining the api-endpoint  
API_ENDPOINT = "http://www.agridata.tn/fr/api/3/action/datastore_upsert"
  
# your API key here 
API_KEY = "c9969b90-25e8-40be-b2f2-9e3889a27cc1"
  
# your source code here 

COL1 = response.json()['data']
df = pandas.DataFrame(COL1)

df = df.rename(columns={'18_X_X_506_min': 'T_min', '18_X_X_506_max': 'T_max','18_X_X_506_avg': 'T_avg'})
df = df.rename(columns={'20_X_X_21_min': 'PR_min', '20_X_X_21_avg': 'PR_avg'})

df = df.rename(columns={'5_X_X_6_sum': 'P_sum'})

df = df.rename(columns={'19_X_X_507_min': 'HR_min', '19_X_X_507_max': 'HR_max','19_X_X_507_avg': 'HR_avg'})

df = df.rename(columns={'3_X_X_143_last': 'DV_last', '3_X_X_143_avg': 'DV_avg'})
df = df.rename(columns={'6_X_X_5_avg': 'VV_avg'})
df = df.rename(columns={'31_X_X_49_max': 'VV_max'})
df = df.rename(columns={'0_X_X_600_avg': 'RS_avg'})
df = df.rename(columns={'26_X_X_25_min': 'DPV_min', '26_X_X_25_avg': 'DPV_avg'})
df = df.rename(columns={'8_X_X_4_time': 'HF'})
df = df.rename(columns={'35_X_X_27_avg': 'DT'})
records=df [["date","T_min","T_max","T_avg","PR_min","VV_max","PR_avg","P_sum","HR_min","DT","HF","HR_max","HR_avg","VV_avg","DV_last","DV_avg","RS_avg","DPV_min","DPV_avg"]].to_json(orient='records')
#print (records)

y = json.loads(records)

# data to be sent to api 
json = {
   
  "id": "4d73b77d-5dff-4e58-bd41-cfebfa7ae7b6",
  "records": y  ,
  "force":"true",
  "method": "upsert"
}
#print (json)

header = {"authorization": "********************************"}


  
# sending post request and saving response as response object 
r = requests.post(url = API_ENDPOINT,headers = header , json = json) 
  
# extracting response text  
pastebin_url = r.text 
print(pastebin_url)






