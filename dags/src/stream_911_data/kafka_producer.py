import uuid
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from sodapy import Socrata
import json


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {}'.format(msg.topic()))

def confluent_kafka_producer(messages, bserver, kafka_topic):
    p = Producer({'bootstrap.servers': bserver})
    record_key = str(uuid.uuid4())
    record_value = json.dumps(messages)
    print('Produce Topics')
    p.produce(kafka_topic, key=record_key, value=record_value, on_delivery=delivery_report)
    p.poll(0)
    p.flush()
    print(f'Message with record {record_key} has been sent to {bserver}')
    print('\n')
    

def generate_stream(**kwargs):
    """ *** Create Topic if not exist *** """
    a = AdminClient({'bootstrap.servers': kwargs['BOOTSTRAP_SERVER']})
    new_topics = [NewTopic(kwargs['TOPIC'], num_partitions=1, replication_factor=1)]
    fs = a.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e} Skipping this section")

    """ *** Access Seattle911 API Endpoint *** """

    client = Socrata(domain = "data.seattle.gov",
                    app_token = kwargs['API_TOKEN'])

    results = client.get_all("33kz-ixgy")
    for item in results:
        confluent_kafka_producer(item, kwargs['BOOTSTRAP_SERVER'], kwargs['TOPIC'])