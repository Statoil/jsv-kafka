from confluent_kafka.avro import AvroConsumer
import logging.handlers
from jskafka.das_fft import DasFft


class ConsumerSubscribe:
    log = logging.getLogger('Kafka ConsumerSubscribe')
    log.setLevel(logging.INFO)
    handler = logging.FileHandler('./consumerSubscribe.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)

    def __init__(self, topic, group_id, client_id='pythonClient'):

        self.consumer = AvroConsumer({
            'bootstrap.servers': 'kbro01:9092, kbro01:9092, kbro01:9092',
            'group.id': group_id,
            'client.id': client_id,
            'schema.registry.url': 'http://ksch01:8081'
        })

        self.consumer.subscribe([topic])

    def __str__(self):
        sb = []
        for key in self.__dict__:
            sb.append("{key}='{value}'".format(key=key, value=self.__dict__[key]))

        return ', '.join(sb)

    def __repr__(self):
        return self.__str__()

    def get_message(self, fft=False):
        self.log.info(f'Start : get_messages({fft})')

        message = self.consumer.poll(10)

        if fft:
            dasfft = DasFft()
            message.value()['fft'] = dasfft.amplitudes_fft(message.value()['amplitudes'])

        self.log.info(f'End : get_messages({fft})')

        return message

    def close(self):
        self.log.info(f'Start : close()')

        self.consumer.close()

        self.log.info(f'End : close()')


