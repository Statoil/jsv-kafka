from unittest import TestCase
import datetime

from jskafka.consumer_subscribe import ConsumerSubscribe
import logging


class TestConsumer(TestCase):
    #logging.basicConfig(level=logging.DEBUG)

    topic = 'Grane10k1'
    #topic = 'Test'
    group_id = 'Python-Group'
    auto_offset_reset = 'earliest'

    def test_get_message(self):

        dy = datetime.datetime.now()

        consumer = ConsumerSubscribe(topic=self.topic, group_id=self.group_id, auto_offset_reset=self.auto_offset_reset)

        i = 1
        run = True
        while run:
            message = consumer.get_message()
            if message != None:
                print(f'{i} partition {message.partition()} offset {message.offset()}')

                if (not i % 100):
                    print('*****************************************')
                    print(f'{i} used {datetime.datetime.now() - dy}')
                    print(f'time pr. massage {(datetime.datetime.now() - dy) / i}')
                    print(message.value())
                    run = False

                i = i + 1

        consumer.close()



