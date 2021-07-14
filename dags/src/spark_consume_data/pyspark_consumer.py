import os
### move to dag
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1,org.elasticsearch:elasticsearch-spark-20_2.10:7.7.0 pyspark-shell'
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
import json
import sys

CLIENT_HOST="10.0.100.92"
CLIENT_PORT="9200"
TOPIC="seattle"
INDEX="calls"
ZOOKEEPER_SERVER="10.0.100.21:2181"
GROUP_ID="kafka-spark-elk-streaming"


global spark
spark = SparkSession.builder.appName("Seattle911Calls").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

ssc = StreamingContext(spark.sparkContext, 5)

es_write_conf = {
                    "es.nodes": CLIENT_HOST,
                    "es.port": CLIENT_PORT,
                    "es.resource": f"{INDEX}/_doc",
                    "es.input.json" : "yes",
                    "mapred.reduce.tasks.speculative.execution": "false",
                    "mapred.map.tasks.speculative.execution": "false",
                    "es.mapping.id": "cad_event_number"
                }

mapping = {
            "mappings": {
                        "properties": {
                                "cad_event_number": {"type": "int"},
                                "event_clearance_description": {"type": "keyword"},
                                "call_type": {"type": "keyboard"},
                                "priority": {"type": "int"},
                                "initial_call_type": {"type": "keyword"},
                                "final_call_type": {"type":"keyword"},
                                "original_time_queued": {"type":"date"},
                                "arrived_time": {"type":"date"},
                                "precinct": {"type":"text"},
                                "sector": {"type":"text"},
                                "beat": {"type":"text"}
                            }
                    }
        }

elastic = Elasticsearch(hosts=[CLIENT_HOST])
response = elastic.indices.create(
                                index=INDEX,
                                body=mapping,
                                ignore=400
                                )


def get_item_structure(item):
    return {
            "cad_event_number": item["cad_event_number"],
            "event_clearance_description": item["event_clearance_description"],
            "call_type": item["call_type"],
            "priority": item["priority"],
            "initial_call_type": item["initial_call_type"],
            "final_call_type": item["final_call_type"],
            "original_time_queued": item["original_time_queued"],
            "arrived_time": item["arrived_time"],
            "precinct": item["precinct"],
            "sector": item["sector"],
            "beat": item["beat"]
        }


def get_messages(key,rdd):
    message_dataframe = spark.read.json(rdd.map(lambda value: json.loads(value[1])))
    if not message_dataframe.rdd.isEmpty():
        analyzed_rdd = message_dataframe.rdd.map(lambda item: get_item_structure(item))
        if False:
            print("********************")
            print(spark.createDataFrame(analyzed_rdd).show(20, False))
            print("********************\n")
        elastic_rdd = analyzed_rdd.map(lambda item: json.dumps(item)).map(lambda x: ('', x))

        elastic_rdd.saveAsNewAPIHadoopFile(
            path='-',
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=es_write_conf)


def main():
    kafkaStream = KafkaUtils.createStream(ssc, ZOOKEEPER_SERVER, GROUP_ID, {TOPIC:1})

    kafkaStream.foreachRDD(get_messages)

    print('Begin Streaming...::')
    ssc.start()
    ssc.awaitTermination()
    print('Done....:::')


if __name__ == "__main__":
    main()