from itertools import takewhile

from DashDB import DashDB
from Downloader import Downloader
from IBMWeather import IBMWeather
import time
import datetime

if __name__ == '__main__':
    '''
    downloader = Downloader()
    cs = Downloader('s000', 'all', '2017-05-20', '2017-05-20')
    data = cs.historicalJSON()
    '''

    ibm = IBMWeather("s000", "23")
    cs = ibm.downloader()


    db = DashDB()
    db.connectionInit()


    tablename = "IBM_s000_20170520"
    #db.dbCreate(tablename, {"time" : "VARCHAR(50)", "temp": "DOUBLE", "pres": "DOUBLE"})

    start = time.time()

    for row in cs['observations']:
        result = db.dbInsert(tablename, [str(row['pressure']), str(row['temp']), str(datetime.datetime.fromtimestamp(row['valid_time_gmt']))])
        if result == False:
            print("ERROR")
    '''
    for row in data:
        result = db.dbInsert(tablename, [row['data']['p0'], row['data']['ta'], row['time']])
        if result == False:
            print("ERROR")
    '''
    end = time.time()
    print(end - start)

    #print db.dbFetch(tablename)


