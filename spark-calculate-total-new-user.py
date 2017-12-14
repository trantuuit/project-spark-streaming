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
spark-submit --packages anguenot:pyspark-cassandra:0.6.0 spark-calculate-total-new-user.py
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
        
        log = sc.cassandraTable("test","fsa_log_visit").select("m_date","userid","fsa","fsid")
        date_temp = getGMT()
        date_temp1 = getNextGMT()
        if log.isEmpty()==False:
            log = log.toDF()
            sql.registerDataFrameAsTable(log, "log")
            log.show()
            
            temp1 = sql.sql("select fsa, userid from log where fsa in"
                +"(select fsa from log where m_date >= %s and m_date < %s and userid !=-1 group by fsa having count(fsa) > 1)"%(date_temp, date_temp1))
            temp1.show()
            sql.registerDataFrameAsTable(temp1, "temp1")
            temp2 = sql.sql("select a.fsa, a.userid from log a where a.m_date >=%s and a.m_date < %s and a.fsa not in (select b.fsa from temp1 b )"%(date_temp, date_temp1))
            sql.registerDataFrameAsTable(temp2, "temp2")
            temp_prev = sql.sql("select fsa, userid from log where m_date <%s"%date_temp)
            temp_prev.show()
            tempsum=temp1.union(temp2)
            print('tempsum')
            tempsum.show()
            result = tempsum.subtract(temp_prev)
            result.show()
            total = result.count()
            # print(result.count())
            rdd = sc.parallelize([{
                "bucket":1,
                "m_date": int(date_temp),
                "newusers": int(total)
            }])
            
        else:
            rdd = sc.parallelize([{
                "bucket":1,
                "m_date": int(date_temp),
                "newusers": 0
            }])
        rdd.saveToCassandra('test','draft_newuser_daily_report')
        time.sleep(2)   
    pass