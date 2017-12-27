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
spark-submit --packages anguenot:pyspark-cassandra:0.7.0 spark-calculate-location.py
"""

if __name__ == '__main__':
    if len(sys.argv) != 1:
        print("Usage: spark-calculate-location.py ", file=sys.stderr)
        exit(-1)
    conf = SparkConf() \
	.setAppName("spark-calculate-location") \
	.set("spark.cassandra.connection.host", "10.88.113.74")
    sc = CassandraSparkContext(conf=conf)
    spark = SparkSession(sc)
    sql = SQLContext(sc)

    while True:
        rdd = sc.cassandraTable("web_analytic","fsa_log_visit").select("location_country_name","location_country_code","fsa")
        # rdd.toDF().show()
        if rdd.isEmpty() == False:
            x = rdd.toDF().dropDuplicates(['fsa'])
            x = x.groupBy(['location_country_name','location_country_code']).count()
            array = []
            for row in x.collect():
                x = {'location_country_name': row['location_country_name'], 
                    'location_country_code': row['location_country_code'], 
                    'location_count':row['count'],'bucket':2}
                array.append(x)
            
            result = sc.parallelize(array)
            result.saveToCassandra('web_analytic','location_report')
            # break
        # break
        # time.sleep(2)
    pass