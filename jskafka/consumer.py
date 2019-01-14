from confluent_kafka import TopicPartition
from confluent_kafka.avro import AvroConsumer
import logging.handlers


class Consumer:
    log = logging.getLogger('das app')
    log.setLevel(logging.INFO)
    handler = logging.FileHandler('./consumer.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)

    def __init__(self, topic, partition):


        self.consumer = AvroConsumer({
            'bootstrap.servers': 'kafkabroker1:9092,kafkabroker2:9092, kafkabroker3:9092',
            'group.id': 'jsvgroupid',
            'schema.registry.url': 'http://kafkabroker1:8081'})

        if(topic != None):
            self.topic_partition = TopicPartition(topic, partition)


    def __str__(self):
        sb = []
        for key in self.__dict__:
            sb.append("{key}='{value}'".format(key=key, value=self.__dict__[key]))

        return ', '.join(sb)

    def __repr__(self):
        return self.__str__()


    def get_message(self, offset):
        self.log.info(f'Start : get_message({offset})')

        self.topic_partition.offset = offset

        self.consumer.assign([self.topic_partition])

        message = self.consumer.poll(10)

        self.consumer.close()

        self.log.info(f'End : get_message({offset})')

        return message


    def seek_from_to(self, start_timestamp, end_timestamp):
        self.log.info(f'Start : seek_from_to({start_timestamp}, {end_timestamp})')

        start_offset = self.get_offset(self.topic_partition, start_timestamp)
        end_offset = self.get_offset(self.topic_partition, end_timestamp)

        self.topic_partition.offset = start_offset

        self.consumer.assign([self.topic_partition])

        messages = []

        while True:
            message = self.consumer.poll(10)
            messages.append(message)
            if (message.offset() >= end_offset):
                self.log.info(f'End : seek_from_to({start_timestamp}, {end_timestamp})')
                return messages

    def get_closest_message(self, timestamp):
        self.log.info(f'Start : get_closest_message({timestamp})')

        offset = self.get_offset(self.topic_partition, timestamp)
        self.topic_partition.offset = offset

        self.consumer.assign([self.topic_partition])

        message = self.consumer.poll(10)

        self.log.info(f'End : get_closest_message({timestamp})')
        return message

    def get_offset(self, topic_partition, timestamp):

        topic_partition.offset = timestamp
        offsets = self.consumer.offsets_for_times([topic_partition])
        return offsets[0].offset

    def get_offsets(self, topic_partition, timestamps):

        i = 0
        topic_partitions = []
        for timestamp in timestamps:

            tp  = TopicPartition(topic_partition.topic, topic_partition.partition)
            tp.offset = timestamp
            topic_partitions.append(tp)

        offsets = self.consumer.offsets_for_times(topic_partitions)
        return offsets

    def get_topics(self):
        self.log.info(f'Start : get_topics()')
        topics = self.consumer.list_topics().topics
        self.log.info(f'Start : get_topics()')
        return topics

