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

    while True:
        rdd = sc.cassandraTable("web_analytic","fsa_log_visit")\
                .select(
                    "config_browser",
                    "config_device",
                    "fsa",
                    "location_browser_lan",
                    "location_country_name",
                    "location_country_code",
                    "m_date",
                    "location_path")
        
        if rdd.isEmpty() == False:
            table_drop = rdd.toDF().dropDuplicates(['fsa'])

            result_config_browser = table_drop.groupBy(['config_browser']).count()
            result_config_device = table_drop.groupBy(['config_device']).count()
            result_browser_language = table_drop.groupBy(['location_browser_lan']).count()
            result_country = table_drop.groupBy(['location_country_name','location_country_code']).count()
            result_location_path = rdd.toDF().groupBy(['location_path']).count()

#-------------------------------------------
            array_config_browser = []
            for row in result_config_browser.collect():
                x = {
                    'config_browser': row['config_browser'], 
                    'browser_count': row['count'],
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
                    'bucket':6
                    }
                array_browser_language.append(x)
            result_location_browser = sc.parallelize(array_browser_language)
#------------------------------------------

            array_country = []
            for row in result_country.collect():
                x = {'location_country_name': row['location_country_name'], 
                    'location_country_code': row['location_country_code'], 
                    'location_count':row['count'],'bucket':2}
                array_country.append(x)
            result_country = sc.parallelize(array_country)


            result_country.saveToCassandra('web_analytic','location_report')
            result_location_browser.saveToCassandra('web_analytic','browser_language_report')
            result_config_device.saveToCassandra('web_analytic','device_report')
            result_config_browser.saveToCassandra('web_analytic','browser_report')
            # break