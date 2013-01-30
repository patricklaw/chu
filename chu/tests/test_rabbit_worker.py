#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
#
#
# Copyright 2012 ShopWiki
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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

# All tests expect a local RabbitMQ server to be running

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

        response = yield gen.Task(future.get)
        
        self.stop()



class WorkerTimeout(Worker):
    def handle_request(self, channel, method, properties, body):
        time.sleep(7) # try to force a timeout
        props = BasicProperties(correlation_id=properties.correlation_id)
        reply_body = json.dumps({'my': 'BODY'})

        # Note that we don't provide an exchange here because the routing key
        # is setup as a "direct" key for RPC.
        channel.basic_publish(exchange='',
                              routing_key=properties.reply_to,
                              properties=props,
                              body=reply_body)

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


class WorkerEmptyReply(Worker):
    def handle_request(self, channel, method, properties, body):
        props = BasicProperties(correlation_id=properties.correlation_id)

        # If this is left as an empty string, the test fails.
        # I am not sure why.
        reply_body = '{}'

        # Note that we don't provide an exchange here because the routing key
        # is setup as a "direct" key for RPC.
        channel.basic_publish(exchange='',
                              routing_key=properties.reply_to,
                              properties=props,
                              body=reply_body)

        # Acknowledge the message was received and processed
        channel.basic_ack(method.delivery_tag)

class TestRPCClientWorkerEmptyReply(AsyncTestCase):
    @gen_wrapper(timeout=10)
    def test_call_no_reply(self):
        client = AsyncTornadoRPCClient('localhost',
                                       self.io_loop)

        rpc_request = RPCRequest(exchange='exch',
                                 routing_key='key',
                                 params=dict(ncc=1701))
        
        worker = WorkerEmptyReply(exchange='exch',
                                  bindings=['key'],
                                  event_io_loop=self.io_loop)
        worker.start()

        listening = yield gen.Task(worker.listening.wait, timeout=5)
        self.assertTrue(listening)

        future = yield gen.Task(client.rpc, rpc_request)
        response = yield gen.Task(future.get)

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
