from datetime import datetime
import csv
import os

class NoaaParser:

    # values are: (begining column, ending column, scaling factor)
    type_of_measurements = {
        'pres0': (99, 104, 10),
        'temp': (87, 92, 10),
        'temp0': (93, 98, 10),
        'winds': (65, 69, 10)
    }

    def __init__(self, path):
        self.path = path

    def parseFile(self, measurement):
        basePath = "DataNOAA/processed"
        textFiles = {}
        writers = {}

        with open("DataNOAA/source/" + self.path) as f:
            prev = ""
            for line in f:
                 dt = datetime.strptime(line[15:27], '%Y%m%d%H%M%S')
                 fileName = str(dt.year) + "-" + str(dt.month)
                 if fileName not in textFiles and fileName not in writers:
                     dirName = basePath + "/" + measurement
                     if not os.path.exists(dirName):
                         os.makedirs(dirName)
                     textFiles[fileName] = open(dirName + "/" + measurement + "_" + fileName + ".csv", "wb")
                     writers[fileName] = csv.writer(textFiles.get(fileName), delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
                 writer = writers.get(fileName)

                 indexes = self.type_of_measurements.get(measurement)
                 if measurement.startswith('temp'):
                     temp_sign = line[indexes[0]:indexes[0]+1]
                     value = float(line[indexes[0]+1:indexes[1]])/indexes[-1]
                     if temp_sign == "-":
                         value = temp_sign + str(value)
                 else:
                     value = float(line[indexes[0]:indexes[1]])/indexes[-1]

                 if dt.minute != 0:
                    dt = dt.replace(minute=0)
                 if dt != prev and value * indexes[-1] < 9999:
                     prev = dt
                     writer.writerow([dt, value])
            for k, v in textFiles.iteritems():
                v.close()


if __name__ == '__main__':
    path = "125660-99999-2016"
    parser = NoaaParser(path)
    for k in parser.type_of_measurements:
        parser.parseFile(k)