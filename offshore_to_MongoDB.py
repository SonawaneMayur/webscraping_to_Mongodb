import csv
import json
import requests
import re
import os
import time
#import JsonToMongoDB as JM
import pymongo
import pprint
import pandas as pd
import Environment as ENV
import updateZip as uz

timestr = ''#time.strftime("%Y-%m-%d-%H")

start_time = time.time()

##read already existed JSON file from folder
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


file_names= uz.update_Zip()  ##downloading the csv files if changed/updated
offshore_file= file_names[0]  ##offshore file name
offshore_updateOn = offshore_file.split('.')[1]  ## Offshore file updated date


screening_file = file_names[1]  ##screenig file name
screening_updateOn = (screening_file.split('.')[0]).split('_')[1] ##screening file updated date

_input_file_offshore, _input_file_screening = ENV.getInputFile(offshore_file,screening_file)  #offshore_leaks.nodes.entity.csv


_output_file = ENV.getOutpuFile()


offshore_csvfile = open(_input_file_offshore,'r', encoding='utf-8')
jsonfile = open(_output_file, 'w')

jsonfile.write('{"offshoreLeaks_UsScreeningList": [')

#read address JSON if exist otherwise assign empty dictionary
addrs_dict = read_addrs_Json()

#require fields in offshore output JSON
fieldnames = ("name", "address", "countries", "incorporation_date", "type")

offshore_reader = csv.DictReader(offshore_csvfile)

db = ENV.connect_MangoDb()    ## creating the connection with MongoDB
timestr = offshore_updateOn

if "offshore_leaks" not in db.collection_names():
    #pass
    #db.offshore_leaks
    mdb = db.offshore_leaks
    mdb.create_index([("name", 1),("countries", 1)],unique=True)
#db.offhsore_leaks.create_index()
if "offshore_leaks_versions" not in db.collection_names():
    #pass
    #db.offshore_leaks_versions
    mdb_v = db.offshore_leaks_versions
    mdb_v.create_index([("name", 1),("countries", 1)],unique=True)


start = True
for row in offshore_reader:
    data={"updatedOn": [timestr]}
    if start:
        #jsonfile.write(',\n')
        start = False
    else:
        jsonfile.write(',\n')

    for item in fieldnames:
        if item in row:
            current_addrs = row[item]
            if item == "address" and current_addrs != "":
                if current_addrs not in addrs_dict:
                    loc, country = find_location(row[item])
                    temp = {"latlang":loc, "country":country}
                    addrs_dict[current_addrs] = temp
                else:
                    loc = addrs_dict[current_addrs]["latlang"]
                    country = addrs_dict[current_addrs]["country"]
                #print(row[item])

                #print(loc,country)
                data[item],data["country_of_residence"] = [loc], [country]

            else:
                if item == "name" or item == "countries":
                    data[item] = row[item] if row[item] != "" else ""
                else:
                    data[item] = [row[item]] if row[item] != "" else [""]
                #data["country_of_residence"] = None

    if "country_of_residence" not in data:
        data["country_of_residence"] = [""]
    #json.dump(data, jsonfile,separators=(',',':')) ## this is the local store for json objects

    ## Check if item exists otherwise update or insert into a MongoDb.collections
    mdb = db.offshore_leaks
    mdb_v = db.offshore_leaks_versions
    mdb_item = mdb.find({"name": data["name"], "countries": data["countries"]})
    mdb_v_item = mdb_v.find({"name": data["name"], "countries": data["countries"]})

    if mdb_item.count() == 0 or mdb_v_item.count() == 0:
        if mdb_item.count() == 0:
            try:
                mdb.insert(data)   ## inserting the each item in MongoDB collection offshore_leaks
            except pymongo.errors.BulkWriteError as e:
                pprint([err for err in e.details['writeErrors'] if err['code'] == 11000])
        if mdb_v_item.count() == 0:
            try:
                mdb_v.insert(data)
            except pymongo.errors.BulkWriteError as e:
                pprint([err for err in e.details['writeErrors'] if err['code'] == 11000])
    else:
        for result in mdb_item[0:1]:
            set_fields = {}
            if result["address"] != data["address"]:
                set_fields["address"] = data["address"][-1]

            if result["country_of_residence"] != data["country_of_residence"]:
                set_fields["country_of_residence"] = data["country_of_residence"][-1]

            if result["incorporation_date"] != data["incorporation_date"]:
                set_fields["incorporation_date"] = data["incorporation_date"][-1]

            if result["type"] != data["type"]:
                set_fields["type"] = data["type"][-1]

            if any(set_fields.values()):
                set_fields["updatedOn"] = timestr
                mdb.update(
                    {"name": data["name"], "countries": data["countries"]},
                        {
                            '$set': set_fields
                    }, upsert=True
                );

                mdb_v.update(
                    {"name": data["name"], "countries": data["countries"]},
                    {
                        '$push': set_fields
                    }, upsert=True
                );





jsonfile.write(']}')
write_addrs_Json(addrs_dict)
end_time = (time.time() - start_time)/60
print("--- %d minutes ---" % end_time)

