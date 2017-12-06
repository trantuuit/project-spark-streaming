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

"""
spark-submit --packages anguenot:pyspark-cassandra:0.6.0,\org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0\ 
 test.py 10.88.113.111:9092 log 
"""


def getValue(x,key,defVal):
    if key not in x:
        return defVal
    if x[key] is None:
        return defVal
    return x[key]

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    conf = SparkConf() \
	.setAppName("spark-streaming") \
	.set("spark.cassandra.connection.host", "10.88.113.74")
    sc = CassandraSparkContext(conf=conf)
    ssc = StreamingContext(sc, 2)
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    # kvs.pprint()
    parsed = kvs.map(lambda x: json.loads(x[1]))
    ob = parsed.map(lambda x: 
        { 
            "userid": getValue(x,'userid','-1'),
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
            # "dma_code": getValue(x,'dma_code','')
        })

    print('-'*30)
    # if ob.count() !=0:
    #     print(ob['userid'])
    ob.pprint('userid')
    # ob.saveToCassandra("test","fsa_log_visit")
    # parsed.pprint()
    # text_counts.pprint()
    print('-'*30)

    # movie = sc.cassandraTable("db","user_model").toDF()
    # print(movie.collect())
    # x = movie[movie.movie_id == "tt2456504"].collect()
    # print(x)
    ssc.start()
    ssc.awaitTermination()
    pass