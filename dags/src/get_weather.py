import requests
from datetime import datetime
import json
import os

url = "https://weatherapi-com.p.rapidapi.com/current.json"

querystring = {"q":"London"}

headers = {
	"X-RapidAPI-Key": "b67d17edf0mshc88b76c457f89e0p16d035jsna9bc5ecdd891",
	"X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

if response.status_code == 200:
    # get the json data
    json_data = response.json()
    file_name = str( datetime.now().date() ) + '.json'
    tot_name = os.path.join( os.path.dirname( __file__ ), 'data', file_name )
    with open( tot_name, 'w' ) as outputfile:
        json.dump( json_data, outputfile )
else:
    print( response.status_code )
    print( 'Error in API call.' )
