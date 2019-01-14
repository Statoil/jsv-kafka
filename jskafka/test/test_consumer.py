from unittest import TestCase
import datetime

from jskafka.consumer import Consumer
import logging


class TestConsumer(TestCase):
    #logging.basicConfig(level=logging.DEBUG)



    def test_get_message(self):

        topic = 'GraneSM2_f15'
        partition = 1

        dy = datetime.datetime.now()

        consumer = Consumer(topic, partition)

        message = consumer.get_message(150)
        self.assertIsNotNone(message.value())

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertTrue(message.value().get('amplitudes').__len__() > 0)

        print(f'partition: {message.partition()}')
        print(f'offset: {message.offset()}')
        print(f'timestamp: {message.timestamp()}')
        ##print(message.value())

    def test_seek_from_to(self):
        topic = 'GraneSM2_f15'
        partition = 1

        start_ts = 1547173195118
        slutt_ts = 1547173295118

        dy = datetime.datetime.now()
        consumer = Consumer(topic, partition)

        messages = consumer.seek_from_to(start_ts, slutt_ts)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertTrue(messages.__len__() > 0)
        self.assertEqual(messages[0].offset(), 50)
        self.assertEqual(messages[-1].offset(), 150)

    def test_get_closest_message(self):
        topic = 'GraneSM2_f15'
        partition = 1

        ts = 1547173195118

        dy = datetime.datetime.now()

        consumer = Consumer(topic, partition)

        message = consumer.get_closest_message(ts)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(message.offset(), 50)
        #print(message.value())

    def test_get_topics(self):

        dy = datetime.datetime.now()

        consumer = Consumer(None, None)

        topics = consumer.get_topics()

        dx = datetime.datetime.now()
        #print(f'Time used {dx - dy}')

        self.assertTrue(topics.__len__() > 0)

