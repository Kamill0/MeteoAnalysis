import csv
import urllib2
import json
import datetime


class IBMWeather:
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
    BASEURL = 'https://twcservice.eu-gb.mybluemix.net/api/weather/v1/geocode/'

    def __init__(self, station, hours):
        self.station = station
        self.lat = self.station_coordinates[station][0]
        self.lon = self.station_coordinates[station][1]
        self.hours = hours
        self.cr = None

    def downloader(self):
        url = self.BASEURL + self.lat + "/" + self.lon + "/observations/timeseries.json?hours=" + self.hours + "&language=pl&units=m"
        username = '7dd7f335-424f-4acf-9bb5-d7c48a8dc9e1'
        password = 'bU1X9hkTmA'
        p = urllib2.HTTPPasswordMgrWithDefaultRealm()

        p.add_password(None, url, username, password)

        handler = urllib2.HTTPBasicAuthHandler(p)
        opener = urllib2.build_opener(handler)
        urllib2.install_opener(opener)
        response = urllib2.urlopen(url).read()

        if not response:
            print("Response was empty")
            return
        self.cr = self.json_loads_byteified(response)
        return self.cr

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
    def printData(self):
        for row in self.cr['observations']:
            #print(row['valid_time_gmt'])
            print datetime.datetime.fromtimestamp(row['valid_time_gmt'])


if __name__ == '__main__':
    ibm = IBMWeather("s000", "23")
    ibm.downloader()
    ibm.printData()