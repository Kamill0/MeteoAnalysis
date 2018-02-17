from datetime import datetime
from os import listdir
import csv
import os

class WfiisDataTools:

    # values are: (begining column, ending column, scaling factor)
    type_of_measurements = {
        'pres0': (99, 104, 10),
        'temp': (87, 92, 10),
        'temp0': (93, 98, 10),
        'winds': (65, 69, 10)
    }

    def __init__(self, path, parent):
        self.parent = parent
        self.path = path


    def hourly(self):

        with open(self.path, 'rb') as f:
            newDir = self.parent + "/hourly"
            if not os.path.exists(newDir):
                os.makedirs(newDir)
            reader = csv.reader(f, delimiter=',', quotechar='|')
            writer = csv.writer(open(newDir + "/h_" + self.path.split('/')[-1], "wb"), delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)

            # get first reader row to extract info]
            row1 = next(reader)
            dt1 = datetime.strptime(row1[0], '%Y-%m-%d %H:%M:%S')

            curHour = dt1.hour
            curDay = dt1.day
            sum = 0
            counter = 0
            prevVal = 0
            for line in reader:
                dt = datetime.strptime(line[0], '%Y-%m-%d %H:%M:%S')
                if dt.hour == curHour:
                    try:
                        sum += float(line[-1])
                    except ValueError:
                        sum += prevVal
                    counter += 1
                else:
                    newDt = dt.replace(day=curDay,hour=curHour, minute=0, second=0)
                    curHour = dt.hour
                    curDay = dt.day
                    if counter == 0:
                        writer.writerow([newDt, 0])
                        # what to do here :(?
                        # until i get a better idea, that's how it is
                    else:
                        val = float("{0:.1f}".format(float(sum) / float(counter)))
                        writer.writerow([newDt, val])
                    try:
                        sum = float(line[-1])
                    except ValueError:
                        sum = prevVal
                    counter = 1
                try:
                    prevVal = float(line[-1])
                except ValueError:
                    "Nothing"
                    # prevVal will store the last value that could have been converted to float, nothing else
                    # we can do here

if __name__ == '__main__':
    path = "Data"
    dirs = listdir(path)
    for d in dirs:
        subDir = path + "/" + d + "/temp"
        files = listdir(subDir)
        for file in files:
            newPath = subDir + "/" + file
            if os.path.isfile(newPath):
                tools = WfiisDataTools(newPath, subDir)
                tools.hourly()
