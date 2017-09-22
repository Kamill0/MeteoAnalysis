from datetime import datetime
import csv

class NoaaParser:
    def __init__(self, path):
        self.path = path

    def parseFile(self):
        basePath = "DataNOAA/processed/"
        textFiles = {}
        writers = {}
        #writer = csv.writer(text_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)

        with open("DataNOAA/source/" + self.path) as f:
            prev = ""
            for line in f:
                 dt = datetime.strptime(line[15:27], '%Y%m%d%H%M%S')
                 fileName = str(dt.year) + "-" + str(dt.month)
                 if fileName not in textFiles and fileName not in writers:
                     textFiles[fileName] = open(basePath + "/temp_" + fileName + ".csv", "wb")
                     writers[fileName] = csv.writer(textFiles.get(fileName), delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
                 writer = writers.get(fileName)
                 airtemp_sign = line[87:88]
                 airtemp_value = float(line[88:92])/10
                 if airtemp_sign == "-":
                     airtemp_value = airtemp_sign + str(airtemp_value)
                 if dt.minute != 0:
                    dt = dt.replace(minute=0)
                 if dt!=prev:
                     #print(dt)
                     prev = dt
                     writer.writerow([dt, airtemp_value])
            for k,v in textFiles.iteritems():
                v.close()


if __name__ == '__main__':
    path = "125660-99999-2017"
    parser = NoaaParser(path)
    parser.parseFile()