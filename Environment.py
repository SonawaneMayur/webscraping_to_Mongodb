from pprint import pprint           # pprint library is used to make the output look more pretty
import pymongo
import keys


directory = "C:\\Users\\Mayur\\Documents\\Assesment\\Sayari_Analytics\\"


##Get Input .csv files
def getInputFile(offshore_file, screening_file):
    input_offshore = "{}{}\\{}".format(directory,offshore_file,'offshore_leaks.nodes.entity.csv')
    input_screening = "{}\\US_Consolidate_Screening_List\\{}".format(directory,screening_file)
    return input_offshore, input_screening


##get location where to save .json file
def getOutpuFile():
    output = "{}file.json".format(directory)
    return output


##get address file
def getAddressFile():
    addrFile = "{}address_latlag.json".format(directory)
    return addrFile


##get location to save address filw
def setAddressFile():
    addrFile = "{}address_latlag.json".format(directory)
    return addrFile


##get url to Google MAP API
def getGeocodeURL():
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    return url

##Get API key to connect Google MAP API
def getAPIKey():
    key = keys.getGoogleAPIKey()
    return key

## Connection for MangoDB on ATLAS
def connect_MangoDb():

    # connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
    client = pymongo.MongoClient('mongodb+srv://{}@cluster0-etwif.mongodb.net/test'.format(keys.getMongoCredentials()))
    db=client.test

    # Issue the serverStatus command and print the results
    serverStatusResult=db.command("serverStatus")
    pprint(serverStatusResult)
    return db


