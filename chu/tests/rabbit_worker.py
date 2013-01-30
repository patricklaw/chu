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

from threading import Thread

from pika.adapters import BlockingConnection
from pika import ConnectionParameters, BasicProperties

from tornado.ioloop import IOLoop
from tornado.gen import engine, Task
from tornado import stack_context

import json

from threading import Event
from chu.connection import AsyncRabbitConnectionBase
from chu.tests.util import AsyncEvent

import logging
logger = logging.getLogger(__name__)

class Worker(Thread):
    rabbit_host = 'localhost'

    def __init__(self, exchange, bindings=[], event_io_loop=None, *args, **kwargs):
        super(Worker, self).__init__(*args, **kwargs)
        self.daemon = True
        self.exchange = exchange
        self.bindings = bindings
        self.event_io_loop = event_io_loop
        self.listening = AsyncEvent(io_loop=self.event_io_loop)

    def run(self, *args, **kwargs):
        self.io_loop = IOLoop()
        self.client = AsyncRabbitConnectionBase(host=self.rabbit_host,
                                                io_loop=self.io_loop)
        self.io_loop.add_callback(stack_context.wrap(self._start_client))
        self.io_loop.start()

    @engine
    def _start_client(self):
        logger.info('Declaring worker exchange')
        yield Task(self.client.exchange_declare,
                   exchange=self.exchange,
                   exchange_type='topic',
                   auto_delete=True)

        logger.info('Declaring worker queue')
        self.queue = yield Task(self.client.queue_declare,
                                exclusive=True,
                                auto_delete=True)

        logger.info('Binding worker keys')
        for routing_key in self.bindings:
            yield Task(self.client.queue_bind,
                       queue=self.queue,
                       exchange=self.exchange,
                       routing_key=routing_key)

        logger.info('Starting worker message consumption')
        yield Task(self.client.basic_consume,
                   consumer_callback=self._handle_message,
                   queue=self.queue)

        self.listening.set()

    def _handle_message(self, channel, method, properties, body):
        self.handle_request(channel, method, properties, json.loads(body))

    def handle_request(self, channel, method, properties, body):
        # This is an example implementation.  Subclasses should override
        # handle_request
        # print "got request with cid: %s" % properties.correlation_id

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


