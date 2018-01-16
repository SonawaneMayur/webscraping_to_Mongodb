import Environment as ENV
import json
import os
import re
import requests

def read_addrs_Json():
    flag = False
    try:
        addrsfile = open(ENV.getAddressFile(), "r")
        addrJson = json.load(addrsfile)
        addrsfile.close()
        #return addrJson
        flag = True
    except Exception as e:
        pass
        #print(e)

    if flag:
        return addrJson
    else:
        addrJson = {}
        return addrJson


##Write or Update new Addresses in Address JSON
def write_addrs_Json(data):
    ## Save our changes to JSON file
    PATH = ENV.setAddressFile()
    ##Create new fileif not exist otherwise update existed one
    action = "w+" if os.path.isfile(PATH) and os.access(PATH, os.R_OK) else "w"

    jsonFile = open(PATH, action)
    jsonFile.write(json.dumps(data))
    jsonFile.close()

##clean address string
def clean_address(addr):
    x = re.search('\d+', addr)
    return addr[x.start(0):]

##Find the (Lattitude,Longitude) & Country of given address string
def find_location(addr):
    url = ENV.getGeocodeURL()

    ##Clean the address string
    if re.search('\d+', addr):
        x = re.search('\d+', addr)
        caddr = addr[x.start(0):]
        #print("clean addrs - ",caddr)
    else: return None,None

    ## Prepare URL using parameters required to fetch location
    params = {'sensor': 'false','address': caddr,'key':ENV.getAPIKey()}
    try:
        r = requests.get(url, params=params)
        results = r.json()['results']
        location = results[0]['geometry']['location']
        loc = str(location['lat']) + "," + str(location['lng'])          ##fetch the Lattutude and Longitude
        i = 1
        while results[0]['address_components'][-i]['types'][0] != 'country':
            i += 1
        country =  results[0]['address_components'][-i]['long_name']       ##fetch the Country

    except Exception as e:
        #print(e)
        return None,None

    return loc,country
