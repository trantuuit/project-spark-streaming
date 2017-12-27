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
spark-submit --packages anguenot:pyspark-cassandra:0.7.0 spark-calculate-device.py
"""

if __name__ == '__main__':
    if len(sys.argv) != 1:
        print("Usage: spark-calculate-device.py ", file=sys.stderr)
        exit(-1)
    conf = SparkConf() \
	.setAppName("spark-calculate-device") \
	.set("spark.cassandra.connection.host", "10.88.113.74")
    sc = CassandraSparkContext(conf=conf)
    spark = SparkSession(sc)
    sql = SQLContext(sc)

    while True:
        rdd = sc.cassandraTable("web_analytic","fsa_log_visit").select("config_device","fsa")
        if rdd.isEmpty() == False:
            x = rdd.toDF().dropDuplicates(['fsa'])
            x = x.groupBy(['config_device']).count()
            array = []
            for row in x.collect():
                x = {
                    'config_device': row['config_device'], 
                    'device_count': row['count'],
                    'bucket':3
                    }
                array.append(x)
            
            result = sc.parallelize(array)
            result.saveToCassandra('web_analytic','device_report')
            # break