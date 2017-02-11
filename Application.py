from itertools import takewhile

from DashDB import DashDB
from Downloader import Downloader
import time

if __name__ == '__main__':
    downloader = Downloader()
    cs = Downloader('s000', 'temp', '2017-01-01', '2017-01-10')
    data = cs.historicalJSON()
    db = DashDB()
    db.connectionInit()


    tablename = "TEMP2017010120170110"
    #db.dbCreate(tablename, {"time" : "VARCHAR(50)", "utc" : "VARCHAR(50)", "temp": "DOUBLE"})

    '''
    start = time.time()
    for row in data:
        result = db.dbInsert(tablename, [row['utc'], row['data']['ta'], row['time']])
        if result == False:
            print("ERROR")

    end = time.time()
    print(end - start) '''

    print db.dbFetch(tablename)


