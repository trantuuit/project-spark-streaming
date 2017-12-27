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
spark-submit --packages anguenot:pyspark-cassandra:0.7.0 spark-calculate-total-new-user.py
"""
if __name__ == '__main__':
    if len(sys.argv) != 1:
        print("Usage: spark-process-data", file=sys.stderr)
        exit(-1)
    conf = SparkConf() \
	.setAppName("spark-calculate-total-new-user") \
	.set("spark.cassandra.connection.host", "10.88.113.74")
    sc = CassandraSparkContext(conf=conf)
    spark = SparkSession(sc)
    sql = SQLContext(sc)
    while True:
        # t0 = time.time()
        # log = sc.cassandraTable("test","fsa_log_visit").select("m_date","userid","fsa","fsid")
        current_date = getGMT()
        future_date = getNextGMT()
        rdd = sc.cassandraTable("web_analytic","fsa_log_visit").select("m_date","userid","fsa","fsid")
        if rdd.isEmpty() == False:
            df = rdd.toDF()
            current_day = df.filter(df.m_date >= current_date).filter(df.m_date < future_date).dropDuplicates(['fsa',"fsid"]).select('fsa','fsid')
            previous_day =  df.filter(df.m_date < current_date).select('fsa','fsid')
            result = current_day.subtract(previous_day)
            total = result.count()
            rdd = sc.parallelize([{
                "bucket":1,
                "m_date": int(current_date),
                "newusers": int(total)
            }])
            rdd.saveToCassandra('web_analytic','newuser_daily_report')
    pass