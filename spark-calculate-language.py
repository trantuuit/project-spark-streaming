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
spark-submit --packages anguenot:pyspark-cassandra:0.7.0 spark-calculate-language.py
"""

if __name__ == '__main__':
    if len(sys.argv) != 1:
        print("Usage: spark-calculate-language.py ", file=sys.stderr)
        exit(-1)
    conf = SparkConf() \
	.setAppName("spark-calculate-language") \
	.set("spark.cassandra.connection.host", "10.88.113.74")
    sc = CassandraSparkContext(conf=conf)
    spark = SparkSession(sc)
    sql = SQLContext(sc)

    while True:
        rdd = sc.cassandraTable("web_analytic","fsa_log_visit").select("location_browser_lan","fsa")
        if rdd.isEmpty() == False:
            x = rdd.toDF().dropDuplicates(['fsa'])
            x = x.groupBy(['location_browser_lan']).count()
            array = []
            for row in x.collect():
                x = {
                    'browser_language': row['location_browser_lan'], 
                    'count': row['count'],
                    'bucket':6
                    }
                array.append(x)
            
            result = sc.parallelize(array)
            result.saveToCassandra('web_analytic','browser_language_report')
            # break