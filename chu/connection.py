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

import pika
from pika.adapters import TornadoConnection
import uuid
from threading import Lock

from tornado import gen
from tornado.gen import Task, Callback, Wait
from tornado.ioloop import IOLoop
from tornado import stack_context

import logging
logger = logging.getLogger(__name__)


class AsyncRabbitConnectionBase(object):
    def __init__(self, host, io_loop=None):
        self.host = host

        self.io_loop = io_loop
        if not self.io_loop:
            logger.info('Using the global IOLoop instance.')
            self.io_loop = IOLoop.instance()
        else:
            logger.info('Using a custom IOLoop.')
        
        self.connect_lock = Lock()

        self.connection_open_callbacks = []
        
        self.connection = None
        self.channel = None
    
    @gen.engine
    def connect(self, callback):
        logger.info('Connecting to the rabbit server.')
        callback((yield gen.Task(self.reconnect)))
    
    @gen.engine
    def ensure_connection(self, callback):
        logger.info('Ensuring that the connection is open.')
        if not (self.connection
                and self.connection.is_open
                and self.channel):
            logger.info('Adding callback to list of callbacks '
                        'waiting for the connection to be open.')
            self.connection_open_callbacks.append(callback)

            logger.info('Calling reconnect().')
            connection = yield gen.Task(self.reconnect)
            logger.info('Reconnect has been called.')
        else:
            logger.info('The connection is already open.')
            callback()
    
    @gen.engine
    def reconnect(self, callback):
        logger.info('Attempting to acquire the connect_lock.')
        if not self.connect_lock.acquire(False):
            logger.info('AsyncRabbitClient.reconnect is already '
                        'attempting to connect (the connect_lock '
                        'could not be acquired).')
            callback()
            return
        
        try:
            logger.info('AsyncRabbitClient.reconnect attempting to '
                        'connect to host: %s' % self.host,
                        extra={'host': self.host})

            params = pika.ConnectionParameters(host=self.host)
        
            key = str(uuid.uuid4())
            TornadoConnection(parameters=params,
                              custom_ioloop=self.io_loop, 
                              on_open_callback=(yield gen.Callback(key)))

            logger.info('Waiting for TornadoConnection to return control '
                        'via on_open_callback.')
            self.connection = yield gen.Wait(key)
            logger.info('Control has been returned.')
            
            logger.info('Opening a channel on the connection.')
            key = str(uuid.uuid4())
            self.connection.channel(on_open_callback=
                                    (yield gen.Callback(key)))

            logger.info('Waiting for connection.channel to return control '
                        'via on_open_callback.')
            self.channel = yield gen.Wait(key)
            logger.info('Control has been returned.')
            
            logger.info('Adding callbacks to warn us when the connection '
                        'has been closed and when backpressure is being '
                        'applied.')
            self.connection.add_on_close_callback(self.on_closed)
            self.connection.add_backpressure_callback(self.on_backpressure)

            logger.info('Adding callbacks that are waiting for an open '
                        'connection to the tornado queue.')
            while self.connection_open_callbacks:
                cb = self.connection_open_callbacks.pop()
                self.io_loop.add_callback(cb)
            logger.info('Done adding callbacks.')

        except Exception as e:
            logger.critical('An unknown exception was raised when trying '
                            'to open a connection to rabbit: %s' %
                            unicode(e))
            raise
        finally:
            logger.info('Releasing the connect lock.')
            self.connect_lock.release()
            callback()

    def on_backpressure(self):
        logger.info('AsyncRabbitClient.on_backpressure: backpressure!')
    
    def on_closed(self, connection):
        logger.info('AsyncRabbitClient.on_close: closed!')
    
    @gen.engine
    def queue_declare(self, callback, **kwargs):
        logger.info('Declaring queue.')
        yield Task(self.ensure_connection)
        kwargs.setdefault('queue', '')
        frame = yield gen.Task(self.channel.queue_declare, **kwargs)
        logger.info('Queue successfully declared.  Frame returned.')
        callback(frame.method.queue)

    @gen.engine
    def exchange_declare(self, callback, **kwargs):
        logger.info('Declaring exchange.')
        yield Task(self.ensure_connection)
        yield gen.Task(self.channel.exchange_declare, **kwargs)
        logger.info('Exchange successfully declared.')
        callback()

    @gen.engine
    def queue_bind(self, exchange, queue, routing_key, callback=None):
        yield Task(self.ensure_connection)
        frame = yield gen.Task(self.channel.queue_bind,
                                exchange=exchange,
                                queue=queue,
                                routing_key=routing_key)
        callback(frame)

    @gen.engine
    def basic_consume(self, queue, consumer_callback=None, no_ack=True, callback=None):
        logger.info('Beginning basic_consume.')
        if not consumer_callback:
            consumer_callback = self.consume_message
        yield Task(self.ensure_connection)

        self.channel.basic_consume(queue=queue,
                                   no_ack=no_ack,
                                   consumer_callback=consumer_callback)
        callback()

    def consume_message(self, channel, method, header, body):
        pass
