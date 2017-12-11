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
from datetime import datetime, timezone, date

def getTimeStamp():
    # year=datetime.utcnow().year
    year=datetime.now().year
    # month=datetime.utcnow().month
    month=datetime.now().month
    # day=datetime.utcnow().day
    day=datetime.now().day
    # utc_zone = tz.gettz('UTC')
    # result = datetime.datetime(2016, 11, 6, 4, tzinfo=datetime.timezone.utc)
    # return datetime.date()
    # return date(2017,12,8).timetuple()
    # return date.today()
    # return datetime(2017,12,8,0,0,0)
    # datetime.datetime.
    # return datetime(year,month,day,tzinfo=timezone.utc)
    result = int(time.mktime(time.strptime('%s-%s-%s' %(year,month,day), '%Y-%m-%d'))) - time.timezone
    # result = int(time.mktime(time.strptime('%s-%s-%s' %(year,month,day), '%Y-%m-%d')))
    # print(result)
    # return str(year)+'-'+str(month)+'-'+str(day)
    # return datetime.datetime
    # return datetime.utcnow().date()
    return result

"""
spark-submit --packages anguenot:pyspark-cassandra:0.6.0 spark-calculate-total-new-user.py
"""
if __name__ == '__main__':
    if len(sys.argv) != 1:
        print("Usage: spark-process-data", file=sys.stderr)
        exit(-1)
    conf = SparkConf() \
	.setAppName("spark-calculate-total-new-user") \
	.set("spark.cassandra.connection.host", "localhost")
    sc = CassandraSparkContext(conf=conf)
    spark = SparkSession(sc)
    sql = SQLContext(sc)
    date_temp = getTimeStamp()
    log = sc.cassandraTable("test","fsa_log_visit").select("date","userid","fsa","fsid").toDF()
    sql.registerDataFrameAsTable(log, "log")
    log.show()
    # print(sql.sql("select * from log where m_date = %s"%date_temp).collect())
    temp1 = sql.sql("select fsa, userid from log where fsa in"
        +"(select fsa from log where date=%s and userid !=-1 group by fsa having count(fsa) > 1)"%date_temp)
    temp1.show()
    sql.registerDataFrameAsTable(temp1, "temp1")
    temp2 = sql.sql("select a.fsa, a.userid from log a where a.date=%s and a.fsa not in (select b.fsa from temp1 b )"%date_temp)
    sql.registerDataFrameAsTable(temp2, "temp2")
    temp_prev = sql.sql("select fsa, userid from log where date <%s"%date_temp)
    temp_prev.show()
    tempsum=temp1.union(temp2)
    print('tempsum')
    tempsum.show()
    result = tempsum.subtract(temp_prev)
    result.show()
    # result = sql.sql("select t1.fsa, t1.userid from temp1 t1 union select t2.fsa, t2.userid from temp2 t2")
    # result.show()
    # result.show()
    # print('----result:%s'%result)
    # print(temp.collect())
    
    # print(sql.sql("select fsa, userid from log where fsa not in temp").collect())
    # while True:
    #     rdd = sc.cassandraTable("test","fsa_log_visit").select("m_date","userid","fsa","fsid")\
    #             .filter(lambda x: int(x['m_date']) == date_temp)

    #     if rdd.isEmpty() == False:
    #         table = rdd.toDF()
    #         table.show(truncate=False)
    #         total=table.dropDuplicates(['fsa',"fsid"]).count()

    #         result = sc.parallelize([{
    #             "bucket":0,
    #             "m_date": int(date_temp),
    #             "users": int(total)
    #         }])
    #     else:
    #         result = sc.parallelize([{
    #             "bucket":0,
    #             "m_date": int(date_temp),
    #             "users": 0
    #         }])
    #     result.saveToCassandra('test','draft_user_daily_report')
    #     time.sleep(2)
    pass