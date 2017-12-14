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
spark-submit --packages anguenot:pyspark-cassandra:0.6.0 spark-calculate-total-user.py
"""
if __name__ == '__main__':
    if len(sys.argv) != 1:
        print("Usage: spark-process-data", file=sys.stderr)
        exit(-1)
    conf = SparkConf() \
	.setAppName("spark-calculate-total-user") \
	.set("spark.cassandra.connection.host", "10.88.113.74")
    sc = CassandraSparkContext(conf=conf)
    spark = SparkSession(sc)
    

    while True:
        date_temp = getGMT()
        rdd = sc.cassandraTable("test","fsa_log_visit").select("m_date","userid","fsa","fsid")\
                .filter(lambda x: getGMT() <= int(x['m_date']) < getNextGMT())

        if rdd.isEmpty() == False:
            table = rdd.toDF()
            table.show(truncate=False)
            total=table.dropDuplicates(['fsa',"fsid"]).count()

            result = sc.parallelize([{
                "bucket":0,
                "m_date": int(date_temp),
                "users": int(total)
            }])
        else:
            result = sc.parallelize([{
                "bucket":0,
                "m_date": int(date_temp),
                "users": 0
            }])
        result.saveToCassandra('test','draft_user_daily_report')
        time.sleep(2)
    pass