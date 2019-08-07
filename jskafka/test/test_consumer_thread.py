from unittest import TestCase
import datetime

from jskafka.consumer_thread import ConsumerThread
import logging

class TestConsumer(TestCase):
    # logging.basicConfig(level=logging.DEBUG)
    topic = 'Test'

    def test_get_message(self):
        partitions = [1, 3, 5]

        dy = datetime.datetime.now()

        consumer = ConsumerThread(self.topic)

        results = consumer.get_message(partitions, 50)
        self.assertIsNotNone(results)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(results.get(1).partition(), 1)
        self.assertEqual(results.get(3).partition(), 3)
        self.assertFalse('fft' in results.get(3).value())
        self.assertTrue('amplitudes' in results.get(3).value())
        self.assertEqual(results.get(5).partition(), 5)

    def test_get_message_fft(self):
        partitions = [1, 3, 5]

        dy = datetime.datetime.now()

        consumer = ConsumerThread(self.topic)

        results = consumer.get_message(partitions, 50, fft=True)
        self.assertIsNotNone(results)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(results.get(1).partition(), 1)
        self.assertEqual(results.get(3).partition(), 3)
        self.assertTrue('fft' in results.get(3).value())
        self.assertTrue('amplitudes' in results.get(3).value())
        self.assertEqual(results.get(5).partition(), 5)


    def test_seek_from_to_timestamps(self):

        partitions = [1, 3, 5]
        #partitions = range(1, 10)

        start_ts = 1547819755110000  # 10
        slutt_ts = 1548063901657000  # 69

        dy = datetime.datetime.now()

        consumer = ConsumerThread(self.topic)

        results = consumer.seek_from_to_timestamps(partitions, start_ts, slutt_ts)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        # partition --> offset
        self.assertTrue(results.__len__() > 0)
        self.assertEqual(results.get(3)[0].partition(), 3)
        self.assertFalse('fft' in results.get(3)[0].value())
        self.assertTrue('amplitudes' in results.get(3)[0].value())
        self.assertEqual(results.get(5)[0].partition(), 5)

    def test_seek_from_to_timestamps_fft(self):

        partitions = [1, 3, 5]
        #partitions = range(1, 10)

        start_ts = 1547819755110000  # 10
        slutt_ts = 1548063901657000  # 69

        dy = datetime.datetime.now()

        consumer = ConsumerThread(self.topic)

        results = consumer.seek_from_to_timestamps(partitions, start_ts, slutt_ts, fft=True)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        # partition --> offset
        self.assertTrue(results.__len__() > 0)
        self.assertEqual(results.get(3)[0].partition(), 3)
        self.assertTrue('fft' in results.get(3)[0].value())
        self.assertTrue('amplitudes' in results.get(3)[0].value())
        self.assertEqual(results.get(5)[0].partition(), 5)



    def test_seek_from_to_offsets(self):


        partitions = [1, 3, 5]
        #partitions = range(1, 10)

        start_offset = 10
        slutt_offset = 69

        dy = datetime.datetime.now()
        consumer = ConsumerThread(self.topic)

        results = consumer.seek_from_to_offsets(partitions, start_offset, slutt_offset)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertTrue(results.__len__() >  0)
        self.assertEqual(results.get(3)[1].partition(), 3)
        self.assertEqual(results.get(3)[2].partition(), 3)
        self.assertEqual(results.get(5)[1].partition(), 5)

