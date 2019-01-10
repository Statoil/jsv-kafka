from unittest import TestCase
import datetime

from jskafka.consumer import Consumer


class TestConsumer(TestCase):

    def test_seek_from_to(self):
        client_id = '1'
        bootstrap_servers = ['kafkabroker1:9092', 'kafkabroker2:9092', 'kafkabroker3:9092']
        topic = 'GraneSM2_f6'
        partition = 1

        start_ts = 1546827595118
        slutt_ts = 1546827645118

        dy = datetime.datetime.now()
        consumer = Consumer(client_id, bootstrap_servers, topic, partition)

        messages = consumer.seek_from_to(start_ts, slutt_ts)

        self.assertTrue(messages.__len__() > 0)
        self.assertEqual(messages[0].offset, 50)
        self.assertEqual(messages[-1].offset, 100)

    def test_get_message(self):
        client_id = '1'
        bootstrap_servers = ['kafkabroker1:9092', 'kafkabroker2:9092', 'kafkabroker3:9092']
        topic = 'dasdata'
        #topic = 'GraneSM2_f6'
        partition = 0

        dy = datetime.datetime.now()

        consumer = Consumer(client_id, bootstrap_servers, topic, partition)

        message = consumer.get_message(1)

        self.assertTrue(message.get('snapshots').__len__() > 0)


    def test_get_closest_message(self):
        client_id = '1'
        bootstrap_servers = ['kafkabroker1:9092', 'kafkabroker2:9092', 'kafkabroker3:9092']
        topic = 'GraneSM2_f6'
        partition = 1

        ts = 1546827595118

        dy = datetime.datetime.now()

        consumer = Consumer(client_id, bootstrap_servers, topic, partition)

        message = consumer.get_closest_message(ts)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(message.offset, 50)

    def test_get_topics(self):
        client_id = '1'
        bootstrap_servers = ['kafkabroker1:9092', 'kafkabroker2:9092', 'kafkabroker3:9092']

        dy = datetime.datetime.now()

        consumer = Consumer(client_id, bootstrap_servers, None, None)

        topics = consumer.get_topics()

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertTrue(topics.__len__() > 0)

