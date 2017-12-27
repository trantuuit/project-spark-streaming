from __future__ import print_function

import sys
import pyspark_cassandra
from pyspark_cassandra import CassandraSparkContext

from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext, SparkConf
from uuid import uuid1
import json
import time 
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
spark-submit --packages anguenot:pyspark-cassandra:0.7.0 spark-calculate-pageview-total-user.py
"""

if __name__ == '__main__':
    if len(sys.argv) != 1:
        print("Usage: spark-calculate-pageview-total-user", file=sys.stderr)
        exit(-1)
    conf = SparkConf() \
	.setAppName("spark-calculate-pageview-total-user") \
	.set("spark.cassandra.connection.host", "10.88.113.74")
    sc = CassandraSparkContext(conf=conf)
    spark = SparkSession(sc)

    while True:
        current_date = getGMT()
        future_date = getNextGMT()
        raw = sc.cassandraTable("web_analytic","fsa_log_visit").select("m_date","userid","fsa","fsid","location_path")
        if raw.isEmpty() == False:
            df = raw.toDF()
            current_day = df.filter( df.m_date >= current_date ).filter(df.m_date < future_date).dropDuplicates(['fsa',"fsid"]).select('fsa','fsid')
            previous_day =  df.filter(df.m_date < current_date).select('fsa','fsid')
            result_new_user = current_day.subtract(previous_day)
            total_newuser = result_new_user.count()
            result_newuser = sc.parallelize([{
                "bucket":1,
                "m_date": int(current_date),
                "newusers": int(total_newuser)
            }])
            

            rdd = raw.filter(lambda x: current_date <= int(x['m_date']) < future_date)
            if rdd.isEmpty() == False:
                table = rdd.toDF()
                total_user=table.dropDuplicates(['fsa',"fsid"]).count()

                result_total_user = sc.parallelize([{
                    "bucket":0,
                    "m_date": int(current_date),
                    "users": int(total_user)
                }])

                pageviews = table.groupBy(['location_path']).count()
                array_pageview = []
                for row in pageviews.collect():
                    x = {
                        'location_path': row['location_path'], 
                        'm_date': int(current_date), 
                        'count':row['count'],
                        'bucket':5
                        }
                    array_pageview.append(x)     
                result_pageview = sc.parallelize(array_pageview)

            result_newuser.saveToCassandra('web_analytic','newuser_daily_report')
            result_pageview.saveToCassandra('web_analytic','page_view_report')
            result_total_user.saveToCassandra('web_analytic','user_daily_report')
