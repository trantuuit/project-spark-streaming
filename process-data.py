from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from uuid import uuid1
# import pyspark_cassandra
'''
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
--packages anguenot:pyspark-cassandra:0.6.0 \ --conf spark.cassandra.connection.host=10.88.113.74
process-data.py 10.88.113.111:9092 log
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 process-data.py 10.88.113.111:9092 log --packages anguenot:pyspark-cassandra:0.6.0 --conf spark.cassandra.connection.host=10.88.113.74

'''
if __name__ == "__main__":
    # if len(sys.argv) != 5:
    #     print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
    #     exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWord")
    ssc = StreamingContext(sc, 2)
    brokers = "10.88.113.111:9092"
    topic = "log"
    # brokers, topic = sys.argv[1:3]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    parsed = kvs.map(lambda x: json.loads(x[1]))
    # Count the number of instance of each tweet text
    # text_counts = parsed.map(lambda x: (x['ip'],1)).reduceByKey(lambda x,y: x + y)
    ob = parsed.map(lambda x: {"fsa":x["clientID"],"fsid":x["_fsid"]})
    print('-'*30)
    # parsed.pprint()
    ob.pprint()
    print('-'*30)
    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #     .map(lambda word: (word, 1)) \
    #     .reduceByKey(lambda a, b: a+b)
    # counts.pprint()
    ssc.start()
    ssc.awaitTermination()
