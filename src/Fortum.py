# coding: utf-8

import pandas as pd
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
from influxdb.client import InfluxDBClientError
import math
import time
import requests
import sys

class Timeseries:
    def __init__(self,series_name):

        self.name = series_name


    def SetInfluxConnection(self,dbhost='localhost',dbport=8086,dbuser='root', dbpassword='root',dbname='test'):

        if dbhost != None:
            self.influxdb = DataFrameClient(dbhost, dbport, dbuser, dbpassword, dbname)
            self.influxdb2 = InfluxDBClient(dbhost, dbport, dbuser, dbpassword, dbname)
            self.dbname = dbname

    def CheckDatabase(self):
        try:
            self.influxdb.create_database(self.dbname)
        except InfluxDBClientError,e:
            if str(e) == "database already exists":
                return True

        return False

    def CheckSeries(self,prop = '*'):

        q = 'select %s from \"%s\" limit 1;' % (prop,self.name)
        result = self.influxdb.query(q)

        print q
        print result

        return result[self.name].index[0]

    def GetLastTimeStamp(self,prop = '*'):
        q = 'select %s from \"%s\" order by time desc limit 1;' % (prop,self.name)
        result = self.influxdb.query(q)
        return result[self.name].index[0]


    def WriteDB(self,df):
        return self.influxdb.write_points(df, self.name)


    def LoadAll(self,start=1199145600, stop = time.time(),period = 60 * 60 * 24 * 7):



        for f in range(start,stop,period):
            data = self.GetDataPeriod(data_id,start, period)
            self.WriteDB(data)

        return




# In[170]:

class Fortum(Timeseries):

    login_cookie = None
    user = None
    password = None
    verbose = True

    #Login
    def login(self):


        data = {"username":self.user,
            "password":self.password,"original_url":"/countries/se/privat/mina-sidor/pages/default.aspx",
            "parent_url": "/frontpage/se/index.htm",
            "langId":"1",
            "login-form-type":"pwd",
            "cod_hack":"&#153;"}

        #Set header
        headers={"Referer":"https://www.fortum.com/blank/pages/loginbox_se_b2c.aspx?parent_url=/frontpage/se/index.htm", "user-agent": "curl/7.43.0"}

        url = "https://www.fortum.com/EAIWebSE_PROD/LoginServlet"

        #Request password cookie
        r = requests.post(url,data=data,headers=headers,allow_redirects=False)

        self.login_cookie = r

    def requestdata(self,meter_id = 11995,date = "2008-01-02", daysback = 7):

        #Request data
        data_url = "https://www.fortum.com/Energikonto/api/ConsumptionsChart/GetHourlyConsumptionData?meteringPointId=%s&chartType=Energy&comparisonType=outdoor&startDate=%s&daysBack=%i"% (meter_id,date,daysback)

        cookies = {'PD-S-SESSION-ID':self.login_cookie.cookies['PD-S-SESSION-ID']}
        #cookies = {"PD-S-SESSION-ID":"2_1_ZoJJi7VXJszUXL1x0tWgKOY704lb6kXWL79uPURfVKZf2rP3"}
        #cookies = r.cookies
        r = requests.get(data_url,cookies = cookies)




        return self.convert_to_df(r.json())

    def convert_to_df(self,json):

        df = pd.DataFrame()


        try:
            df = df.from_dict(json[u'HourlyConsumptionChartValues'])
        except KeyError:
            print "Error in parsing data"
            print json

            return None

        #Convert date and set as index
        df.index = pd.to_datetime(df["From"])
        df = df.drop("From",1)
        df.index.name = "time"

        return df

    def GetDataPeriod(self, data_id, start = 1199145600, period = 60 * 60 * 24 * 7.0):

        if self.login_cookie == None:
            self.login()

        date = time.strftime("%Y-%m-%d", time.localtime(start + period))

        daysback = math.ceil( period / (24*60*60) )

        #print date
        #print daysback

        data_url = "https://www.fortum.com/Energikonto/api/ConsumptionsChart/GetHourlyConsumptionData?meteringPointId=%s&chartType=Energy&comparisonType=outdoor&startDate=%s&daysBack=%i"% (data_id,date,daysback)
        cookies = {'PD-S-SESSION-ID':self.login_cookie.cookies['PD-S-SESSION-ID']}

        r = requests.get(data_url,cookies = cookies)

        #Retry if failed
        if r.status_code != 200:
            r = requests.get(data_url,cookies = cookies)
        if r.status_code != 200:
            r = requests.get(data_url,cookies = cookies)
        if r.status_code != 200:
            print "Request failed!"

        json = r.json()

        #print json

        return self.convert_to_df(json)


    # AH: MffbdnVR81b%2BCU2EqzRaOSeOU8zQ%2FCLq
    def GetMeterPoints(self, customerId = "MffbdnVR81bjIbrZgODUYbyf6s5nBJd4"):

        if self.login_cookie == None:
            self.login()

        data_url = "https://www.fortum.com/Energikonto//FacilityList/GetMeteringPoints?customerId=%s&viewType=consumption&product=H" % customerId
        cookies = {'PD-S-SESSION-ID':self.login_cookie.cookies['PD-S-SESSION-ID']}

        r = requests.get(data_url,cookies = cookies)

        json = r.json()

        print json

    def test(self, customerId = "MffbdnVR81bjIbrZgODUYbyf6s5nBJd4"):

        if self.login_cookie == None:
            self.login()

        data_url = "https://www.fortum.com/mittkonto/account/Account"
        cookies = {'PD-S-SESSION-ID':self.login_cookie.cookies['PD-S-SESSION-ID']}

        r = requests.post(data_url,cookies = cookies)

        return r

    # Månadsavläst el https://www.fortum.com/mittkonto/export/ConsumptionExporter?deliverySiteId=735999102110651915&tstype=cons&from=20140630%2000:00&to=20160101%2000:00&samp=1y&displayTemp=false&displayContvol=true&displaySpot=false&displayCompare=false&period=all
    # Timavläst el https://www.fortum.com/mittkonto/export/ConsumptionExporter?deliverySiteId=735999102109355046&tstype=cons&from=20160220%2023:00&to=&samp=1h&displayTemp=false&displayContvol=true&displaySpot=false&displayCompare=false&period=1d

    def ImportMissing(self, period = 60 * 60 * 24 * 7):
        try:
            last = self.GetLastTimeStamp().value / 10 ** 9
        except:
            last = 1199145600

        self.ImportAll(None,last,None,period)

        return

    def ImportAll(self,data_id = None, start = 1199145600, stop = None, period = 60 * 60 * 24 * 7):

        if data_id == None:
            data_id = self.name

        if stop == None:
            stop = int(time.time())

        now = time.time()
        count = 0

        if self.verbose:
            print "Downloading data from %i to %i" % (start,stop)

        for f in range(start,stop,period):

            completed = 100* (f - start)/(stop-start)
            delta = time.time() - now

            if self.verbose:
                if delta > 10:
                    timeleft = int(  (100 - completed) * (delta / completed)   )
                    print("\rCompleted: %0.0f%% (%i seconds left)" % (completed,timeleft)),
                else:
                    print("\rCompleted: %0.0f%% " % completed),

                sys.stdout.flush()


            data = self.GetDataPeriod(data_id,f,period)

            #Retry
            if type(data) != pd.core.frame.DataFrame :
                time.sleep(3)
                print "Retrying..."
                data = self.GetDataPeriod(data_id,f,period)

            if type(data) == pd.core.frame.DataFrame:

                data = data[data.Status == "Verklig"]

                count += data.shape[0]

                self.WriteDB(data)


        completed = (100* (f-start))/(stop-start)

        if self.verbose:
            print("\rCompleted: %0.0f%%  \r  " % completed),

            delta = time.time() - now

            print("\rTask completed in %0.0f s %i rows written" % (delta,count))

        return
