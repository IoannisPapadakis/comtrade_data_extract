"""
Comtrade API Extract
Nathan Goldschlag
December 17, 2014
Version 1.0
Written in Python 2.7

This python program extracts trade data from the Comtrade API. 

"""
## IMPORT LIBRARIES
import urllib
import time
import json
import os 
from os import listdir
from os.path import isfile, join

workDir = 'c:/Users/ngold/Documents/python_library/comtrade_data/'

# read in JSON file of country codes as a list
f = open('reporterAreas.json','r')
areas = json.load(f)
countryIDs = []
for i in areas['results']:
    countryIDs.append(str(i['id']))
countryIDs.remove('all')

# define the rg parameter, trade flow (default = all): The most common area 1 (imports) and 2 (exports)
dtypes = ['1','2']
time.sleep(3600)
# track the number of calls made
t0 = time.time()
callsThisHour = 1

baseurl = 'http://comtrade.un.org/api/get?max=50000&type=C&freq=A&px=S2&ps=2012,2011,2010,2009&r={0}&p=all&rg={1}&cc=AG2&fmt=csv'
for c in countryIDs:
    print 'country:', c, '\n', 'time this hour:', time.time()-t0, '\n','calls this hour:',callsThisHour
    for dt in dtypes:
        # create the URL and submit url
        url= baseurl.format(c,dt)
        apiResponse =urllib.urlopen(url)
        # parse return output
        csv = apiResponse.read()
        csvstr = str(csv).strip("b'")

        # store output
        lines = csvstr.split('\r\n')
        f = open(workDir+'comtrade_'+c+'_'+dt+'.csv','w')
        for line in lines:
            f.write(line+'\n')
        f.close()

        # track the number of calls this hour
        callsThisHour+=1
        time.sleep(3)
        timePassed = time.time() - t0
        
        # if hour limit reached, sleep for the remainer of the hour
        if timePassed<3600 and callsThisHour>98:
            print 'sleeping...'
            time.sleep(3700-timePassed)
            print 'awake'
            # reset hour and number of calls
            t0=time.time()
            callsThisHour=0

# glue csvs together into one file
files = [x for x in listdir(workDir)]
files = [x for x in files if '.csv' in x]

fout = open(workDir+'comtrade_all.csv','a')
for line in open(workDir+files[0],'r'):
    fout.write(line)
for filename in files:
    fin = open(workDir+filename,'r')
    fin.next()
    for line in fin:
        fout.write(line)
    fin.close()
fout.close()




