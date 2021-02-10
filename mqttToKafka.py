from kafka import KafkaProducer
import paho.mqtt.client as mqtt
from kafka.errors import KafkaError, NoBrokersAvailable
from json import dumps
from os import getenv
from logging import basicConfig, info, DEBUG
from enum import Enum


class Topic(Enum):
    TopicMqtt = "hostqueue"
    TopicKafka = "grogu"


def SendMessageKafka(message):
    producer.send(Topic.TopicKafka.value, message)
    print("Sending message to kafka: %s" % message)


def OnConnect(client, userdata, flags, rc):
    print("Connected with result code {0}".format(str(rc)))
    client.subscribe(Topic.TopicMqtt.value)


def OnMessage(client, userdata, msg):
    SendMessageKafka(msg.payload.decode())
    print("Message received-> " + msg.topic + " " + str(msg.payload))


if __name__ == '__main__':
    # basicConfig(format='%(asctime)s %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=DEBUG)
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: dumps(x).encode('utf-8'))
    client = mqtt.Client(client_id="home_connector_%s" % getenv("localhost"))
    client.on_connect = OnConnect
    client.on_message = OnMessage
    client.connect("127.0.0.1", 1883, 60)
    client.loop_forever()
