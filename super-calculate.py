from __future__ import print_function

import sys
import pyspark_cassandra
from pyspark_cassandra import CassandraSparkContext

from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext, SparkConf
from uuid import uuid1
import json
import time
from dateutil import tz
from datetime import datetime, timezone, date, timedelta

"""
spark-submit --packages anguenot:pyspark-cassandra:0.7.0 super-calculate.py
"""

def getGMT():
    year=datetime.now().year
    month=datetime.now().month
    day=datetime.now().day
    result = int(time.mktime(time.strptime('%s-%s-%s' %(year,month,day), '%Y-%m-%d'))) - time.timezone
    return result

def getNextGMT():
    result = datetime.now() + timedelta(days=1)
    year = result.year
    month = result.month
    day = result.day
    return int(time.mktime(time.strptime('%s-%s-%s' %(year,month,day), '%Y-%m-%d'))) - time.timezone
    pass

if __name__ == '__main__':
    if len(sys.argv) != 1:
        print("Usage: spark-calculate-browser.py ", file=sys.stderr)
        exit(-1)
    conf = SparkConf() \
	.setAppName("spark-calculate-browser") \
	.set("spark.cassandra.connection.host", "10.88.113.74")
    sc = CassandraSparkContext(conf=conf)
    spark = SparkSession(sc)
    sql = SQLContext(sc)

    i = 1511740800
    while i <= 1514505600:
    # while True:
        date_temp = i
        i = i + 86400
        # current_date = getGMT()
        # future_date = getNextGMT()
        rdd = sc.cassandraTable("web_analytic","fsa_log_visit")\
                .select(
                    "config_browser",
                    "config_device",
                    "fsa",
                    "location_browser_lan",
                    "location_country_name",
                    "location_country_code",
                    "m_date",
                    "location_path",
                    "location_city_name",
                    "config_resolution",
                    "location_os"
                    )\
                .filter(lambda x: date_temp <= int(x['m_date']) < i)
                # .filter(lambda x: current_date <= int(x['m_date']) < future_date)
        # 1514332800
        if rdd.isEmpty() == False:
            table_drop = rdd.toDF().dropDuplicates(['fsa'])
            # table_drop.show()
            # break
            result_config_browser = table_drop.groupBy(['config_browser']).count()
            result_config_device = table_drop.groupBy(['config_device']).count()
            result_browser_language = table_drop.groupBy(['location_browser_lan']).count()
            result_country = table_drop.groupBy(['location_country_name','location_country_code']).count()
            result_location_path = table_drop.groupBy(['location_path']).count()
            result_location_city = table_drop.groupBy(['location_city_name']).count()
            result_location_os = table_drop.groupBy(['location_os']).count()
            result_config_resolution = table_drop.groupBy(['config_resolution']).count()
#-------------------------------------------
            array_config_browser = []
            for row in result_config_browser.collect():
                x = {
                    'config_browser': row['config_browser'], 
                    'browser_count': row['count'],
                    # 'm_date': current_date,
                    'm_date': date_temp,
                    'bucket': 4
                    }
                array_config_browser.append(x)
            result_config_browser = sc.parallelize(array_config_browser)
#------------------------------------------

            array_config_device = []
            for row in result_config_device.collect():
                x = {
                    'config_device': row['config_device'], 
                    'device_count': row['count'],
                    'm_date': date_temp,
                    # 'm_date': current_date,
                    'bucket': 3
                }
                array_config_device.append(x)
            result_config_device = sc.parallelize(array_config_device)
#------------------------------------------

            array_browser_language = []
            for row in result_browser_language.collect():
                x = {
                    'browser_language': row['location_browser_lan'], 
                    'count': row['count'],
                    'm_date': date_temp,
                    # 'm_date': current_date,
                    'bucket':6
                    }
                array_browser_language.append(x)
            result_location_browser = sc.parallelize(array_browser_language)
#------------------------------------------

            array_country = []
            for row in result_country.collect():
                x = {'location_country_name': row['location_country_name'], 
                    'location_country_code': row['location_country_code'], 
                    'location_count':row['count'],
                    'm_date': date_temp,
                    # 'm_date': current_date,
                    'bucket':2
                    }
                array_country.append(x)
            result_country = sc.parallelize(array_country)
#----------------------------------------

            array_city = []
            for row in result_location_city.collect():
                x = {
                    'city_name': row['location_city_name'],
                    'bucket': 7,
                    'm_date': date_temp,
                    # 'm_date': current_date,
                    'count': row['count']
                }

                array_city.append(x)
            result_location_city = sc.parallelize(array_city)

#---------------------------------------

            array_location_os = []
            for row in result_location_os.collect():
                x = {
                    'bucket': 8,
                    'm_date': date_temp,
                    # 'm_date': current_date,
                    'os_name': row['location_os'],
                    'count': row['count']
                }
                array_location_os.append(x)
            result_location_os = sc.parallelize(array_location_os)

#----------------------------------------

            array_config_resolution = []
            for row in result_config_resolution.collect():
                x = {
                    'bucket': 9,
                    'm_date': date_temp,
                    # 'm_date': current_date,
                    'screen': row['config_resolution'],
                    'count': row['count']
                }
                array_config_resolution.append(x)
            result_config_resolution = sc.parallelize(array_config_resolution)

            result_config_resolution.saveToCassandra('web_analytic','system_screen_report')
            result_location_os.saveToCassandra('web_analytic','os_report')
            result_location_city.saveToCassandra('web_analytic','city_report')
            result_country.saveToCassandra('web_analytic','location_report')
            result_location_browser.saveToCassandra('web_analytic','browser_language_report')
            result_config_device.saveToCassandra('web_analytic','device_report')
            result_config_browser.saveToCassandra('web_analytic','browser_report')
            #break
