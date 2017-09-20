import csv
import urllib2
import json
import socket
import os
import datetime
import copy

class MyException(Exception):
    pass

class Downloader:
    BASEURL = 'http://mech.fis.agh.edu.pl/meteo/rest/'

    type_of_measurements = [
        'pres0',
        'temp',
        'temp0',
        'humi',
        'rain1',
        'rain',
        'windd',
        'winds',
        'windg'
    ]

    station_coordinates = {
        's000': ("50.0670", "19.9129"),
        's001': ("50.0618", "19.9291"),
        's002': ("50.057", "19.933"),
        's003': ("49.8303", "19.9439"),
        's004': ("50.275", "19.575"),
        's005': ("50.13967", "19.39783"),
        's006': ("49.7154", "20.4161"),
        's007': ("50.0154", "20.9944"),
        's008': ("49.9994", "20.928"),
        's009': ("49.851", "19.344"),
        's010': ("49.62161", "20.69555"),
        's011': ("50.0447", "19.2111"),
        's012': ("50.0464", "19.92219"),
        's013': ("49.473253", "20.025243"),
        's014': ("50.017", "20.9878"),
        's015': ("50.0622", "19.9243"),
        's016': ("49.848768", "20.794452"),
        's017': ("49.9732", "19.8363"),
        's018': ("49.6528", "21.1623"),
        's501': ("50.01154", "19.9486")
    }

    def __init__(self, station="", typeOfMeasurement="", dateFrom="", dateTo="", dateStats="", monthly = False):
        self.station = station
        self.typeOfMeasurement = typeOfMeasurement
        self.dateFrom = dateFrom
        self.start = dateFrom
        self.dateTo = dateTo
        self.end = dateTo
        self.dateStats = dateStats
        self.cr = None
        self.monthly = monthly

    # gets rid off the unicode sign in json
    def _byteify(self, data, ignore_dicts=False):
        if isinstance(data, unicode):
            return data.encode('utf-8')
        if isinstance(data, list):
            return [self._byteify(item, ignore_dicts=True) for item in data]
        if isinstance(data, dict) and not ignore_dicts:
            return {
                self._byteify(key, ignore_dicts=True): self._byteify(value, ignore_dicts=True)
                for key, value in data.iteritems()
                }
        return data

    def json_loads_byteified(self, json_text):
        return self._byteify(
            json.loads(json_text, object_hook=self._byteify),
            ignore_dicts=True
        )

    def connection(self, webendpoint):
        try:
            response = urllib2.urlopen(webendpoint, timeout=10)
            return response
        except urllib2.URLError as e:
            print(type(e))  # not catch
        except socket.timeout as e:
            print(type(e))  # caught
            raise MyException("There was an error: %r" % e)

    def downloadData(self, webendpoint):
        response = urllib2.urlopen(webendpoint).read()
        if not response:
            print("Response was empty")
            return
        self.cr = self.json_loads_byteified(response)

    def last_day_of_month(self, any_day):
        next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
        return next_month - datetime.timedelta(days=next_month.day)

    def downloadAsCSVData(self, webendpoint):
        connection = self.connection(webendpoint)
        self.cr = csv.reader(connection)
        rows = []
        for row in self.cr:
            rows.append(row)

        if len(rows) != 0:
            filename = self.station + "_" + self.typeOfMeasurement + "_" + self.start + "_" + self.end
            dirname = "Data/" + self.station + "/" + self.typeOfMeasurement
            if not os.path.exists(dirname):
                os.makedirs(dirname)

            text_file = open(dirname + "/" + filename + ".csv", "wb")
            writer = csv.writer(text_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for row in rows:
                writer.writerow(row)

            text_file.close()
        else:
            print("Response for: " + webendpoint + " was empty")

    def printData(self):
        for row in self.cr:
            print row['data']['ta']

    # selected data for all available stations [JSON]
    def getAllStations(self):
        allStationsUrl = self.BASEURL + 'json/info/'
        self.downloadData(allStationsUrl)
        return self.cr

    # all data for a specific station [JSON]
    def getDetailedStation(self):
        detailedStationUrl = self.BASEURL + 'json/desc/' + self.station
        self.downloadData(detailedStationUrl)
        return self.cr

    # current data for specific station (last 10 measures)
    def currentDataStation(self):
        currentDataForStationUrl = self.BASEURL + 'json/last/' + self.station
        self.downloadData(currentDataForStationUrl)

    # historical data for specific station [JSON]
    def historicalJSON(self):
        historicalJsonUrl = self.BASEURL + 'json/' + self.typeOfMeasurement + '/' + self.station + '/' + self.dateFrom + '/' + self.dateTo
        self.downloadData(historicalJsonUrl)
        return self.cr

    # historical data for specific station [CSV]
    def historicalCSV(self):
        if self.monthly:
            dFrom = datetime.datetime.strptime(self.dateFrom, "%Y-%m-%d").date()
            dTo = datetime.datetime.strptime(self.dateTo, "%Y-%m-%d").date()
            dToLastDay = self.last_day_of_month(dFrom)
            while dTo > dToLastDay:
                self.start = str(dFrom)
                self.end = str(dToLastDay)
                historicalCsvUrl = self.BASEURL + 'csv/' + self.typeOfMeasurement + '/' + self.station + '/' + self.start + '/' + self.end
                self.downloadAsCSVData(historicalCsvUrl)
                dFrom = dToLastDay + datetime.timedelta(days=1)
                dToLastDay = self.last_day_of_month(dFrom)
            self.start = str(dFrom)
            self.end = str(dTo)
            historicalCsvUrl = self.BASEURL + 'csv/' + self.typeOfMeasurement + '/' + self.station + '/' + str(dFrom) + '/' + str(dTo)
            self.downloadAsCSVData(historicalCsvUrl)
        else:
            historicalCsvUrl = self.BASEURL + 'csv/' + self.typeOfMeasurement + '/' + self.station + '/' + self.dateFrom + '/' + self.dateTo
            self.downloadAsCSVData(historicalCsvUrl)


    # statistic for specific station: how many measures were registered [JSON]
    def statistic(self):
        statisticUrl = self.BASEURL + 'json/stat/' + self.station + '/' + self.dateStats
        self.downloadData(statisticUrl)

    # aggregated by hour data for specific station [JSON]
    def aggregated(self):
        aggregatedUrl = self.BASEURL + 'agg/' + self.typeOfMeasurement + '/' + self.station + '/' + self.dateFrom + '/' + self.dateTo
        self.downloadData(aggregatedUrl)


if __name__ == '__main__':
    coordinates = Downloader.station_coordinates
    tom = Downloader.type_of_measurements

    for k in coordinates:
        print(k)
        for t in tom:
            print(t)
            cs = Downloader(k, t, '2017-01-01', '2017-07-31', monthly = True)
            cs.historicalCSV()
    #cs.printData()
