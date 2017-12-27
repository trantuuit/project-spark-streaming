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

"""
spark-submit --packages anguenot:pyspark-cassandra:0.7.0 spark-calculate-pageview.py
"""

if __name__ == '__main__':
    if len(sys.argv) != 1:
        print("Usage: spark-calculate-pageview", file=sys.stderr)
        exit(-1)
    conf = SparkConf() \
	.setAppName("spark-calculate-pageview") \
	.set("spark.cassandra.connection.host", "10.88.113.74")
    sc = CassandraSparkContext(conf=conf)
    spark = SparkSession(sc)

    while True:
        current_date = getGMT()
        future_date = getNextGMT()
        rdd = sc.cassandraTable("web_analytic","fsa_log_visit").select("m_date","location_path")\
                .filter(lambda x: current_date <= int(x['m_date']) < future_date)
        if rdd.isEmpty() == False:
            x = rdd.toDF().groupBy(['location_path']).count()
            # x.show()
            array = []
            for row in x.collect():
                x = {
                    'location_path': row['location_path'], 
                    'm_date': int(current_date), 
                    'count':row['count'],
                    'bucket':5}
                array.append(x)     
            result = sc.parallelize(array)
            result.saveToCassandra('web_analytic','page_view_report')
            # break
            pass