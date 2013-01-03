from tornado.ioloop import IOLoop
from tornado import gen
from tornado.testing import AsyncTestCase

import uuid
import simplejson as json
from datetime import timedelta
from chu.rpc import AsyncTornadoRPCClient, RPCRequest, RPCTimeoutError
from chu.tests.rabbit_worker import Worker
from chu.tests.util import gen_wrapper, AsyncEvent

from multiprocessing import Process
from pika.adapters import BlockingConnection
from pika import ConnectionParameters, BasicProperties
import json
import time
from threading import Event


class TestRPCClient(AsyncTestCase):
    
    @gen_wrapper(timeout=10)
    def test_call(self):
        client = AsyncTornadoRPCClient('localhost',
                                       self.io_loop)

        rpc_request = RPCRequest(exchange='exch',
                                 routing_key='key',
                                 params=dict(ncc=1701))
        
        worker = Worker(exchange='exch',
                        bindings=['key'],
                        event_io_loop=self.io_loop)
        worker.start()
        listening = yield gen.Task(worker.listening.wait, timeout=2)
        self.assertTrue(listening)

        future = yield gen.Task(client.rpc, rpc_request)
        try:
            response = yield gen.Task(future.get)
        except RPCTimeoutError as e:
            print e
        
        self.stop()



class WorkerTimeout(Worker):
    def handle_request(self, channel, method, properties, body):
        # This is an example implementation.  Subclasses should override
        # handle_request
        # print "got request with cid: %s" % properties.correlation_id

        time.sleep(7) # try to force a timeout
        props = BasicProperties(correlation_id=properties.correlation_id)
        reply_body = json.dumps({'my': 'BODY'})

        # Note that we don't provide an exchange here because the routing key
        # is setup as a "direct" key for RPC.
        channel.basic_publish(exchange='',
                              routing_key=properties.reply_to,
                              properties=props,
                              body=reply_body)

        # print "sent reply to: %s" % properties.reply_to

        # Acknowledge the message was received and processed
        channel.basic_ack(method.delivery_tag)


class TestRPCClientWorkerTimeout(AsyncTestCase):

    @gen_wrapper(timeout=10)
    def test_call_timeout(self):
        client = AsyncTornadoRPCClient('localhost',
                                       self.io_loop)

        rpc_request = RPCRequest(exchange='exch',
                                 routing_key='key',
                                 params=dict(ncc=1701))
        
        worker = WorkerTimeout(exchange='exch',
                               bindings=['key'],
                               event_io_loop=self.io_loop)
        worker.start()
        listening = yield gen.Task(worker.listening.wait, timeout=2)
        self.assertTrue(listening)

        future = yield gen.Task(client.rpc, rpc_request)

        # make sure we timed out
        with self.assertRaises(RPCTimeoutError):
            response = yield gen.Task(future.get, timeout=3)
        
        self.stop()


class WorkerNoReply(Worker):
    def handle_request(self, channel, method, properties, body):
        # This is an example implementation.  Subclasses should override
        # handle_request
        # print "got request with cid: %s" % properties.correlation_id

        props = BasicProperties(correlation_id=properties.correlation_id)
        reply_body = ''

        # Note that we don't provide an exchange here because the routing key
        # is setup as a "direct" key for RPC.
        channel.basic_publish(exchange='',
                                   routing_key=properties.reply_to,
                                   properties=props,
                                   body=reply_body)

        # print "sent reply to: %s" % properties.reply_to

        # Acknowledge the message was received and processed
        channel.basic_ack(method.delivery_tag)


class TestRPCClientWorkerNoReply(AsyncTestCase):

    @gen_wrapper(timeout=10)
    def test_call_no_reply(self):
        client = AsyncTornadoRPCClient('localhost',
                                       self.io_loop)

        rpc_request = RPCRequest(exchange='exch',
                                 routing_key='key',
                                 params=dict(ncc=1701))
        
        worker = WorkerNoReply(exchange='exch',
                               bindings=['key'],
                               event_io_loop=self.io_loop)
        worker.start()

        listening = yield gen.Task(worker.listening.wait, timeout=5)
        self.assertTrue(listening)

        future = yield gen.Task(client.rpc, rpc_request)
        try:
            response = yield gen.Task(future.get)
        except RPCTimeoutError as e:
            print e
        
        self.stop()


class WorkerConfirmMessage(Worker):
    def __init__(self, *args, **kwargs):
        super(WorkerConfirmMessage, self).__init__(*args, **kwargs)
        self.receive_event = AsyncEvent(io_loop=self.event_io_loop)

    def handle_request(self, channel, method, properties, body):
        channel.basic_ack(method.delivery_tag)
        self.receive_event.set()

class TestRPCClientBasicPublish(AsyncTestCase):

    @gen_wrapper(timeout=20)
    def test_call_no_reply(self):
        client = AsyncTornadoRPCClient('localhost',
                                       self.io_loop)

        rpc_request = RPCRequest(exchange='exch',
                                 routing_key='key',
                                 params=dict(ncc=1701))
        
        worker = WorkerConfirmMessage(exchange='exch',
                                      bindings=['key'],
                                      event_io_loop=self.io_loop)
        worker.start()

        listening = yield gen.Task(worker.listening.wait, timeout=2)
        self.assertTrue(listening)

        yield gen.Task(client.basic_publish, rpc_request)

        event_value = yield gen.Task(worker.receive_event.wait, timeout=3)

        self.assertTrue(event_value)
        self.stop()