from unittest import TestCase
import datetime

from jskafka.consumer_thread import ConsumerThread
import logging


class TestConsumer(TestCase):
    # logging.basicConfig(level=logging.DEBUG)

    def test_get_message(self):
        topic = 'GraneSM2_f15'
        partitions = [1, 3, 5]

        dy = datetime.datetime.now()

        consumer = ConsumerThread(topic)

        results = consumer.get_message(partitions, 50)
        self.assertIsNotNone(results)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(results.get(1).partition(), 1)
        self.assertEqual(results.get(3).partition(), 3)
        self.assertEqual(results.get(5).partition(), 5)

    def test_seek_from_to_timestamps(self):
        topic = 'GraneSM2_f15'
        #partitions = [1, 3, 5]
        partitions = range(1, 10)

        start_ts = 1547173125118
        slutt_ts = 1547173214118

        dy = datetime.datetime.now()

        consumer = ConsumerThread(topic)

        results = consumer.seek_from_to_timestamps(partitions, start_ts, slutt_ts)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        # partition --> offset
        self.assertTrue(results.__len__() > 0)
        self.assertEqual(results.get(3)[1].partition(), 3)
        self.assertEqual(results.get(5)[1].partition(), 5)


    def test_seek_from_to_offsets(self):
        topic = 'GraneSM2_f15'
        partition = 1
        # partitions = [1, 3, 5]
        partitions = range(1, 10)

        start_offset = 10
        slutt_offset = 69

        dy = datetime.datetime.now()
        consumer = ConsumerThread(topic,)

        results = consumer.seek_from_to_offsets(partitions, start_offset, slutt_offset)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertTrue(results.__len__() >  0)
        self.assertEqual(results.get(3)[1].partition(), 3)
        self.assertEqual(results.get(3)[2].partition(), 3)
        self.assertEqual(results.get(5)[1].partition(), 5)

