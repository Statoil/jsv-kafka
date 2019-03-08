from unittest import TestCase
import datetime

from jskafka.consumer_subscribe import ConsumerSubscribe
import logging


class TestConsumer(TestCase):
    #logging.basicConfig(level=logging.DEBUG)

    #topic = 'RandomSM2_t2'
    topic = 'BHGE02'
    group_id = 'jsv-kafka32'

    def test_get_message(self):

        dy = datetime.datetime.now()

        consumer = ConsumerSubscribe(self.topic, self.group_id)

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

                i = i + 1
            run = False



