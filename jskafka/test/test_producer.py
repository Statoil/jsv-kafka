from unittest import TestCase
import datetime

from jskafka.producer import Producer


class TestProducer(TestCase):

    def test_seek_from_to(self):
        bootstrap_servers = ['kafkabroker1:9092', 'kafkabroker2:9092', 'kafkabroker3:9092']
        topic = 'dasdata'

        producer = Producer(bootstrap_servers)

        events =  producer.sendAvroFile(topic, 'files/small.avro')
        producer.wait_for_events(events)

        print(f'topic : {producer.get_ok().topic}')
        print(f'partition : {producer.get_ok().partition}')
        print(f'offset : {producer.get_ok().offset}')
        self.assertIsNotNone(producer.get_ok())
        self.assertIsNone(producer.get_error())






