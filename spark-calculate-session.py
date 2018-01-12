from __future__ import print_function

import sys


from pyspark_cassandra import CassandraSparkContext

from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import lit,col, lag,coalesce,sum, count,when
from pyspark.sql.window import Window
import json

from datetime import datetime

def getGMT():
    now=datetime.now()
    return int((now.replace(hour=0,minute=0,second=0,microsecond=0)- datetime(1970,1,1)).total_seconds())

def getNextGMT():   
    now=datetime.now()
    return 86400 + int((now.replace(hour=0,minute=0,second=0,microsecond=0)- datetime(1970,1,1)).total_seconds())
    pass
"""
spark-submit --packages anguenot:pyspark-cassandra:0.7.0 spark-calculate-session.py
"""
if __name__ == '__main__':
    if len(sys.argv) != 1:
        print("Usage: spark-process-data", file=sys.stderr)
        exit(-1)
    conf = SparkConf() \
    .setAppName("spark-calculate-session") \
    .set("spark.cassandra.connection.host", "10.88.113.74")
    sc = CassandraSparkContext(conf=conf)
    spark = SparkSession(sc)
    sql = SQLContext(sc)

    while True:

        lower_time_bound =getGMT() # current day
        upper_time_bound =getNextGMT() # Next day
        time_expire=30 # 30 seconds
        fsa_log_past_twodate = sql.read.format("org.apache.spark.sql.cassandra").\
                load(keyspace="web_analytic", table="fsa_log_past_twodate")
        # type(fsa_log_past_twodate)

        current_day_fsa_log=fsa_log_past_twodate.filter("m_date >= %s and m_date < %s"%(str(lower_time_bound),str(upper_time_bound)))
        tmp_df =current_day_fsa_log.select("fsa","m_date","location_path").withColumn("count",lit(0))   
        w = Window.partitionBy("fsa").orderBy("m_date")

        diff = when(coalesce(col("m_date") - lag("m_date", 1).over(w),lit(0)).cast("long")>time_expire,0)\
                .otherwise(coalesce(col("m_date") - lag("m_date", 1).over(w),lit(0)).cast("long"))
        tmp_df=tmp_df\
            .withColumn("duration", diff)\
            .withColumn("count",when(col("duration")==0,1).otherwise(lit(0)))\
            .groupby("fsa")\
            .agg(\
                sum("duration").alias("duration"),\
                sum("count").alias("nb_session")\
                )\
            .withColumn("m_date",lit(lower_time_bound))
        tmp_df.write.\
                format("org.apache.spark.sql.cassandra").\
                options(table="session_overview", keyspace="web_analytic").\
                save(mode="append")

    pass
