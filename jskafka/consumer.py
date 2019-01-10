from kafka import KafkaConsumer, TopicPartition
from io import BytesIO
#import avro.schema
#import avro.io
from fastavro import reader


class Consumer:
    '''
               .......

               Parameters
               ----------
                bootstrap_servers: 'host[:port]' string (or list of 'host[:port]'
                    strings) that the consumer should contact to bootstrap initial
                    cluster metadata. This does not have to be the full node list.
                    It just needs to have at least one broker that will respond to a
                    Metadata API Request. Default port is 9092. If no servers are
                    specified, will default to localhost:9092.
                client_id (str): A name for this client. This string is passed in
                    each request to servers and can be used to identify specific
                    server-side log entries that correspond to this client. Also
                    submitted to GroupCoordinator for logging with respect to
                    consumer group administration. Default: 'kafka-python-{version}'.
                topic (str): topic where to get the message from.
                partition (int): partition where to get the message from.

       '''

    def __init__(self, client_id, bootstrap_servers, topic, partition):
        self.consumer = KafkaConsumer(
            client_id=client_id,
            bootstrap_servers=bootstrap_servers,
            enable_auto_commit=True
        )

        self.topic_partition = TopicPartition(topic, partition)
        self.consumer.assign([self.topic_partition])
        #with open("files/das2.avsc", "rb") as f:
        #    self.schema = avro.schema.Parse(f.read())



    def __str__(self):
        sb = []
        for key in self.__dict__:
            sb.append("{key}='{value}'".format(key=key, value=self.__dict__[key]))

        return ', '.join(sb)

    def __repr__(self):
        return self.__str__()

    def seek_from_to(self, start_timestamp, end_timestamp):

        start_offset = self.get_offset(self.topic_partition, start_timestamp)
        end_offset = self.get_offset(self.topic_partition, end_timestamp)

        self.consumer.seek(self.topic_partition, start_offset)

        messages = []
        for message in self.consumer:
            messages.append(message)
            if (message.offset >= end_offset):
                return messages

    def get_closest_message(self, timestamp):

        offset = self.get_offset(self.topic_partition, timestamp)
        self.consumer.seek(self.topic_partition, offset)

        for message in self.consumer:
            return message

    def get_message(self, offset):


        self.consumer.seek(self.topic_partition, offset)

        for message in self.consumer:
            #bytes_reader = BytesIO(message.value)
            #decoder = avro.io.BinaryDecoder(bytes_reader)
            #reader = avro.io.DatumReader(self.schema)
            try:

                #return reader.read(decoder)
                return self.fromAvro(message.value)
            except Exception as e:
                print(e)

    def get_topics(self):
        return self.consumer.topics()

    def get_offset(self, topic_partition, timestamp):

        offsets = self.consumer.offsets_for_times({topic_partition: timestamp})
        off = offsets.get(topic_partition)
        return off.offset

    def fromAvro(self, data):
        stream = BytesIO(data)
        avro_reader = reader(stream)
        for record in avro_reader:
            return record
