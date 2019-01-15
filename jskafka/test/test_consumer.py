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

        message = consumer.get_message(10)
        self.assertIsNotNone(message.value())

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertTrue(message.value().get('amplitudes').__len__() > 0)

        print(f'partition: {message.partition()}')
        print(f'offset: {message.offset()}')
        print(f'timestamp: {message.timestamp()}')
        ##print(message.value())

    def test_seek_from_to_timestamps(self):
        topic = 'GraneSM2_f15'
        partition = 1

        start_ts = 1547173125118
        slutt_ts = 1547173214118

        dy = datetime.datetime.now()
        consumer = Consumer(topic, partition)

        messages = consumer.seek_from_to_timestamps(start_ts, slutt_ts)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(messages.__len__(),  60)
        self.assertEqual(messages[0].offset(), 10)
        self.assertEqual(messages[-1].offset(), 69)


    def test_seek_from_to_offsets(self):
        topic = 'GraneSM2_f15'
        partition = 1

        start_offset = 10
        slutt_offset = 69

        dy = datetime.datetime.now()
        consumer = Consumer(topic, partition)

        messages = consumer.seek_from_to_offsets(start_offset, slutt_offset)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(messages.__len__(),  60)
        self.assertEqual(messages[0].offset(), 10)
        self.assertEqual(messages[-1].offset(), 69)


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

