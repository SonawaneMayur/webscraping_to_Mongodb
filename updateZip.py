import re
import os
import requests
from bs4 import BeautifulSoup
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
import glob
import csv


offshore_leaks_url = "https://offshoreleaks.icij.org/pages/database"

screening_url = "https://api.trade.gov/consolidated_screening_list/search.csv?api_key=OHZYuksFHSFao8jDXTkfiypO"

directory = "C://Users//Mayur//Documents//Assesment//Sayari_Analytics//"

files = []
def update_Zip():

    if offshore_leaks_url:
        r = requests.get(offshore_leaks_url)

        # create beautiful-soup object
        soup = BeautifulSoup(r.content, 'html5lib')

        # find all links on web-page
        links = soup.findAll(href=re.compile("\.zip$"))

        for link in links:
            n = link['href'].split("/")
            s = n[-1].split(".")
            if s[0] == 'csv_offshore_leaks':
                if not os.path.isfile("C://Users//Mayur//Documents//Assesment//Sayari_Analytics//{}.{}//offshore_leaks.nodes.entity.csv".format(s[0],s[1])):
                    file_directory = "C://Users//Mayur//Documents//Assesment//Sayari_Analytics//{}.{}".format(s[0],s[1])
                    if not os.path.exists(file_directory):
                        os.makedirs(file_directory)

                    print("Dowloading and unpacking new csv- {}".format(link['href']))
                    with urlopen(link['href']) as zipresp:
                        with ZipFile(BytesIO(zipresp.read())) as zfile:
                            zfile.extractall(file_directory)
                            #offshore_file = "{}.{}".format(s[0], s[1])
                            #break

                offshore_file=  "{}.{}".format(s[0],s[1])
                print(offshore_file)
                files.append(offshore_file)
                break

    if screening_url:

        list_of_files = glob.glob('{}US_Consolidate_Screening_List//*'.format(directory))  # * means all if need specific format then *.csv
        latest_file = max(list_of_files, key=os.path.getctime)
        latest_file = latest_file.split("\\")[1]
        print(latest_file)

        with requests.Session() as s:
            download = s.get(screening_url)

        file_name = download.headers['Content-Disposition'].split('/')[1]

        if latest_file != file_name:
            temp = '{}US_Consolidate_Screening_List//'.format(directory)
            with open('{}//{}'.format(temp,file_name), 'wb') as temp_file:
                #temp_file.TemporaryFile(mode="w+")
                temp_file.write(download.content)


            screening_file_path = file_name
            print(screening_file_path)
            files.append(screening_file_path)


        else:
            screening_file_path = latest_file
            print(screening_file_path)
            files.append(screening_file_path)

    return files

