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
spark-submit --packages anguenot:pyspark-cassandra:0.7.0 spark-calculate-last-like.py
"""

if __name__ == '__main__':
    if len(sys.argv) != 1:
        print("Usage: spark-calculate-last-like.py ", file=sys.stderr)
        exit(-1)
    conf = SparkConf() \
	.setAppName("spark-calculate-last-like") \
	.set("spark.cassandra.connection.host", "10.88.113.74")
    sc = CassandraSparkContext(conf=conf)
    spark = SparkSession(sc)
    sql = SQLContext(sc)

    while True:
        rdd = sc.cassandraTable("db","user_event_model").select("config_browser","m_date")
        if rdd.isEmpty() == False:
            x = rdd.toDF().groupBy(['config_browser']).count()
            array = []
            for row in x.collect():
                x = {
                    'config_browser': row['config_browser'], 
                    'browser_count': row['count'],
                    'bucket':4
                    }
                array.append(x)
            
            result = sc.parallelize(array)
            result.saveToCassandra('test','browser_report')