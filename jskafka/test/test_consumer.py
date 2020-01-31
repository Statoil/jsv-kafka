from unittest import TestCase
import datetime

from jskafka.consumer import Consumer
import logging
from jskafka.constant import Constant


class TestConsumer(TestCase):
    #logging.basicConfig(level=logging.DEBUG)

    topic = '1234-amp'

    def test_get_message(self):

        partition = 0

        dy = datetime.datetime.now()

        consumer = Consumer(self.topic, partition, bootstrap_servers=Constant.BOOTSTRAP_SERVERS_DEV, schema_registry_url=Constant.SCHEMA_REGISTRY_URL_DEV)

        message = consumer.get_message(3)
        self.assertIsNotNone(message.value())

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertFalse('fft' in message.value())
        self.assertTrue('amplitudesFloat' in message.value())
        self.assertTrue(message.value().get('amplitudesFloat').__len__() > 0)
        print(message.value().get('amplitudesFloat'))

        print(f'partition: {message.partition()}')
        print(f'offset: {message.offset()}')
        print(f'timestamp: {message.timestamp()}')
        ##print(message.value())

    def test_get_message_fft(self):

        partition = 0

        dy = datetime.datetime.now()

        consumer = Consumer(self.topic, partition)

        message = consumer.get_message(10, fft=True)
        self.assertIsNotNone(message.value())

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertTrue('fft' in message.value())
        self.assertTrue('amplitudes' in message.value())

        self.assertTrue(message.value().get('amplitudes').__len__() > 0)
        self.assertTrue(message.value().get('fft').__len__() > 0)

        print(f'partition: {message.partition()}')
        print(f'offset: {message.offset()}')
        print(f'timestamp: {message.timestamp()}')
        ##print(message.value())

    def test_seek_from_to_timestamps(self):
        partition = 0

        start_ts = 1563783956345  # 5
        end_ts = 1563786943094  # 69

        dy = datetime.datetime.now()
        consumer = Consumer(self.topic, partition)

        messages = consumer.seek_from_to_timestamps(start_ts, end_ts)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(messages[0].timestamp()[1], start_ts)
        self.assertEqual(messages[-1].timestamp()[1], end_ts)
        self.assertFalse('fft' in messages[0].value())
        self.assertTrue('amplitudes' in messages[0].value())
        self.assertFalse('fft' in messages[-1].value())
        self.assertTrue('amplitudes' in messages[-1].value())

    def test_seek_from_to_timestamps_fft(self):
        partition = 0

        start_ts = 1563783956345  # 5
        end_ts = 1563786943094  # 69

        dy = datetime.datetime.now()

        dy = datetime.datetime.now()
        consumer = Consumer(self.topic, partition)

        messages = consumer.seek_from_to_timestamps(start_ts, end_ts, fft=True)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(messages[0].timestamp()[1], start_ts)
        self.assertEqual(messages[-1].timestamp()[1], end_ts)
        self.assertTrue('fft' in messages[0].value())
        self.assertTrue('amplitudes' in messages[0].value())
        self.assertTrue('fft' in messages[-1].value())
        self.assertTrue('amplitudes' in messages[-1].value())

    def test_seek_from_to_offsets(self):

        partition = 0

        start_offset = 10
        slutt_offset = 69

        dy = datetime.datetime.now()
        consumer = Consumer(self.topic, partition)

        messages = consumer.seek_from_to_offsets(start_offset, slutt_offset)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(messages.__len__(), 60)
        self.assertEqual(messages[0].offset(), 10)
        self.assertEqual(messages[-1].offset(), 69)
        self.assertFalse('fft' in messages[0].value())
        self.assertTrue('amplitudes' in messages[0].value())
        self.assertFalse('fft' in messages[-1].value())
        self.assertTrue('amplitudes' in messages[-1].value())

    def test_seek_from_to_offsets_fft(self):

        partition = 0

        start_offset = 10
        slutt_offset = 69

        dy = datetime.datetime.now()
        consumer = Consumer(self.topic, partition)

        messages = consumer.seek_from_to_offsets(start_offset, slutt_offset, fft=True)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(messages.__len__(), 60)
        self.assertEqual(messages[0].offset(), 10)
        self.assertEqual(messages[-1].offset(), 69)
        self.assertTrue('fft' in messages[0].value())
        self.assertTrue('amplitudes' in messages[0].value())
        self.assertTrue('fft' in messages[-1].value())
        self.assertTrue('amplitudes' in messages[-1].value())

    def test_get_closest_message(self):
        partition = 0

        ts = 1551953503714

        dy = datetime.datetime.now()

        consumer = Consumer(self.topic, partition)

        message = consumer.get_closest_message(ts)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(message.offset(), 0)
        self.assertFalse('fft' in message.value())
        self.assertTrue('amplitudes' in message.value())

    def test_get_closest_message_fft(self):
        partition = 0

        ts = 1551953503714

        dy = datetime.datetime.now()

        consumer = Consumer(self.topic, partition)

        message = consumer.get_closest_message(ts, fft=True)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(message.offset(), 0)
        self.assertTrue('fft' in message.value())
        self.assertTrue('amplitudes' in message.value())


    def test_get_topics(self):

        dy = datetime.datetime.now()

        consumer = Consumer(None, None)

        topics = consumer.get_topics()

        dx = datetime.datetime.now()
        #print(f'Time used {dx - dy}')

        self.assertTrue(topics.__len__() > 0)


