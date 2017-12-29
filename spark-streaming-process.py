from __future__ import print_function

import sys
import pyspark_cassandra
import pyspark_cassandra.streaming
from pyspark_cassandra import CassandraSparkContext

from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
import json
import time
# import datetime
from dateutil import tz
from datetime import datetime, timezone, date
from operator import add
"""
spark-submit --packages anguenot:pyspark-cassandra:0.7.0,\org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 spark-streaming-process.py 10.88.113.111:9092 log 
"""


def getValue(x,key,defVal):
    if key not in x:
        return defVal
    if x[key] is None:
        return defVal
    return x[key]

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
    # return datetime.now().date()
    return int(datetime.now().timestamp())
    # return result

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    conf = SparkConf() \
	.setAppName("spark-streaming") \
	.set("spark.cassandra.connection.host", "10.88.113.74")
    sc = CassandraSparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    # kvs.pprint()
    parsed = kvs.map(lambda x: json.loads(x[1]))
    # parsed.pprint()
    ob = parsed.map(lambda x: 
        { 
            "m_date": getTimeStamp(),
            "userid": getValue(x,'userId','-1'),
            "fsa": x["clientID"],
            "fsid": x["_fsid"],
            "location_ipv4": x['ip'] ,
            "location_browser_lan": x['language'],
            "location_country_code": x['country']['country_code'],
            "location_country_name": x['country']['country_name'],
            "location_browser_en": x['encoding'],
            "config_resolution": x['screenResolution'],
            "config_color_depth": x['screenColors'],
            "config_viewport_size": x['viewportSize'],
            "config_java": x['javaEnabled'],
            "config_browser": x["config_browser"],
            "config_device": x["config_device"],
            "location_path": x["location"],
            "location_city_name": x["city"]['city'],
            "location_os": x['location_os']
            
            # "dma_code": getValue(x,'dma_code','')
        })
    
    # joined = ob.joinWithCassandraTable('web_analytic', 'fsa_log_visit').on("fsa").select("fsa","m_date")
    # joined.reduceByKey(add).pprint()
    # joined.pprint()
    ob.pprint()
    ob.saveToCassandra("web_analytic","fsa_log_visit")
    ssc.start()
    ssc.awaitTermination()
    pass
