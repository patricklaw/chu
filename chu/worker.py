from tornado import gen
from chu.connection import AsyncRabbitConnectionBase

class Worker(AsyncRabbitConnectionBase):
    @gen.engine
    def consume_queue(self, queue, no_ack=False, callback=None):
        yield gen.Task(self.basic_consume,
                       consumer_callback=self.consume_message,
                       queue=queue,
                       no_ack=no_ack)

    def consume_message(self, channel, method, properties, body):
        raise NotImplemented('consume_message should be implemented '
                             'by subclasses of AsyncSimpleConsumer.')
