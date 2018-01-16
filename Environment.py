from pprint import pprint           # pprint library is used to make the output look more pretty
import pymongo


directory = "C:\\Users\\Mayur\\Documents\\Assesment\\Sayari_Analytics\\"

def getInputFile(offshore_file, screening_file):
    input_offshore = "{}{}\\{}".format(directory,offshore_file,'offshore_leaks.nodes.entity.csv')
    input_screening = "{}\\US_Consolidate_Screening_List\\{}".format(directory,screening_file)
    return input_offshore, input_screening

def getOutpuFile():
    output = "{}file.json".format(directory)
    return output

def getAddressFile():
    addrFile = "{}address_latlag.json".format(directory)
    return addrFile

def setAddressFile():
    addrFile = "{}address_latlag.json".format(directory)
    return addrFile

def getGeocodeURL():
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    return url

def getAPIKey():
    key = ''
    return key

def connect_MangoDb():

    # connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
    client = pymongo.MongoClient('mongodb+srv://USERNAME:PASSWORD@cluster0-etwif.mongodb.net/test')
    db=client.test

    # Issue the serverStatus command and print the results
    serverStatusResult=db.command("serverStatus")
    pprint(serverStatusResult)
    return db

