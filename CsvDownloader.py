import csv
import urllib2
import json


class CSVDownloader:
    BASEURL = 'http://mech.fis.agh.edu.pl/meteo/rest/'

    def __init__(self, station="", typeOfMeasurement="", dateFrom="", dateTo="", dateStats=""):
        self.station = station
        self.typeOfMeasurement = typeOfMeasurement
        self.dateFrom = dateFrom
        self.dateTo = dateTo
        self.dateStats = dateStats
        self.cr = None

    def _byteify(self, data, ignore_dicts=False):
        # if this is a unicode string, return its string representation
        if isinstance(data, unicode):
            return data.encode('utf-8')
        # if this is a list of values, return list of byteified values
        if isinstance(data, list):
            return [self._byteify(item, ignore_dicts=True) for item in data]
        # if this is a dictionary, return dictionary of byteified keys and values
        # but only if we haven't already byteified it
        if isinstance(data, dict) and not ignore_dicts:
            return {
                self._byteify(key, ignore_dicts=True): self._byteify(value, ignore_dicts=True)
                for key, value in data.iteritems()
                }
        # if it's anything else, return it in its original form
        return data

    def json_loads_byteified(self, json_text):
        return self._byteify(
            json.loads(json_text, object_hook=self._byteify),
            ignore_dicts=True
        )

    def downloadData(self, webendpoint):
        response = urllib2.urlopen(webendpoint).read()
        self.cr = self.json_loads_byteified(response)

    def downloadAsCSVData(self, webendpoint):
        response = urllib2.urlopen(webendpoint)
        self.cr = csv.reader(response)

    def printData(self):
        for row in self.cr:
            print row

    # selected data for all available stations [JSON]
    def getAllStations(self):
        allStationsUrl = self.BASEURL + 'json/info/'
        self.downloadData(allStationsUrl)

    # all data for a specific station [JSON]
    def getDetailedStation(self):
        detailedStationUrl = self.BASEURL + 'json/desc/' + self.station
        self.downloadData(detailedStationUrl)

    # current data for specific station (last 10 measures)
    def currentDataStation(self):
        currentDataForStationUrl = self.BASEURL + 'json/last/' + self.station
        self.downloadData(currentDataForStationUrl)

    # historical data for specific station [JSON]
    def historicalJSON(self):
        historicalJsonUrl = self.BASEURL + 'json/' + self.typeOfMeasurement + '/' + self.station + '/' + self.dateFrom + '/' + self.dateTo
        self.downloadData(historicalJsonUrl)

    # historical data for specific station [CSV]
    def historicalCSV(self):
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
    cs = CSVDownloader('s000', 'temp', '2017-01-01', '2017-01-10')
    cs.historicalJSON()
    cs.printData()