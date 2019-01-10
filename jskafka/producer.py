from kafka import KafkaProducer




class Producer:
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
       '''

    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers, compression_type='gzip', max_request_size=100000000, buffer_memory= 100000000)
        self.ok = None
        self.error = None

    def __str__(self):
        sb = []
        for key in self.__dict__:
            sb.append("{key}='{value}'".format(key=key, value=self.__dict__[key]))

        return ', '.join(sb)

    def __repr__(self):
        return self.__str__()

    def get_ok(self):
        return self.ok

    def get_error(self):
        return self.error

    def wait_for_events(self, events):
        for event in events:
            event.wait()

    def sendAvroFile(self, topic, filePath):
        def on_send_success(event, record_metadata):
            self.ok = record_metadata
            event.set()

        def on_send_error(excp):
            self.error = excp

        import threading
        import functools

        events = []

        with open(filePath, 'rb') as read_file:
            event = threading.Event()
            callback_with_event = functools.partial(on_send_success, event)

            self.producer.send(topic, read_file.read()).add_callback(callback_with_event).add_errback(on_send_error)

        self.producer.close()
        return events

