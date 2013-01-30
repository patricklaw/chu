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

from datetime import timedelta
from functools import partial
from threading import Lock
import numbers
import uuid

from tornado import gen
from tornado.gen import Task, Callback, Wait
from tornado.ioloop import IOLoop
from tornado import stack_context

import pika
import simplejson as json

from chu.connection import AsyncRabbitConnectionBase

import logging
logger = logging.getLogger(__name__)


class RPCTimeoutError(Exception):
    '''
    Raised when a call to :meth:`.RPCResponseFuture.wait` times out.
    '''
    pass

class RPCErrorResponse(object):
    pass

class RPCRequest(object):
    '''
    A wrapper object around the exchange, routing_key, and a dictionary
    of parameters that will be json encoded into the body of the rabbit
    message.

    :class:`RPCRequest` is hashable and can therefore be used as a key
    in a dictionary.
    '''

    def __init__(self, exchange, routing_key, params, timeout=None):
        self.exchange = exchange
        self.routing_key = routing_key
        self.params = params
        self.json_params = json.dumps(params, sort_keys=True)
        # if timeout is a number, treat it as seconds
        self.timeout = timeout
    
    def __hash__(self):
        if not hasattr(self, '_hash'):
            routing_key_hash = hash(self.routing_key)
            params_hash = hash(self.json_params)
            self._hash = hash((routing_key_hash, params_hash))
        return self._hash

class RPCResponseFuture(object):
    '''
    A convenience object for using :class:`AsyncTornadoRPCClient` in a
    non-blocking manner within a Tornado handler.  Calls to
    :meth:`~AsyncTornadoRPCClient.rpc` will return an instance of
    :class:`RPCResponseFuture`.  The user may then call
    :meth:`RPCResponseFuture.get` in order to retrieve the result of the
    RPC.  Using :class:`tornado.gen.Task` is recommended for calling
    :meth:`~RPCResponseFuture.get`.
    '''
    def __init__(self, cid, timeout=None, io_loop=None):
        self.cid = cid
        self.response_received = False
        self.response = None
        self.timeout = timeout
        self.timed_out = False
        self.wait_callback = None
        self.io_loop = io_loop
        if not self.io_loop:
            self.io_loop = IOLoop.instance()

    @gen.engine
    def get(self, callback, timeout=None):
        '''
        Wait for the RPC associated with this :class:`RPCResponseFuture`
        to return a result.  When the result is received, resolve the
        task by calling the passed in ``callback``.

        :param callback: The callback that will be called with the RPC
            response upon completion of the RPC.  It is recommended that
            this not be passed in directly, but rather that
            :meth:`~.get` be called as a function passed to
            :class:`tornado.gen.Task`.

        :param timeout: The amount of time to wait before raising an
            :exc:`RPCTimeoutError` to indicate that the RPC has timed
            out.  This can be a number or a :class:`timedelta`
            object.  If it is a number, it will be treated as
            seconds.
        '''

        if self.response_received:
            logger.info('Response has already been received, return '
                        'the value immediately.')
            callback(self.response)
        else:
            if self.timeout and not timeout:
                timeout = self.timeout
            elif not self.timeout and not timeout:
                timeout = timedelta(seconds=6)

            key = uuid.uuid4()
            self.wait_callback = yield gen.Callback(key)

            logger.info('Response has not been received yet.  Adding '
                        'timeout to the io_loop in case the response '
                        'times out.')

            if isinstance(timeout, numbers.Real):
                timeout = timedelta(seconds=timeout)
            self.io_loop.add_timeout(timeout, self.timeout_callback)

            logger.info('Waiting for the response.')
            yield gen.Wait(key)

            if self.timed_out:
                raise RPCTimeoutError('Future waiting for message with cid: '
                                      '"%s" timed out' % str(self.cid))
            elif self.response_received:
                logger.info('Response received successfully.')
                callback(self.response)
            else:
                raise Exception("Neither timed out nor response received")

    def response_callback(self, response):
        logger.info('Response callback called.')
        self.response_received = True
        self.response = response
        if self.wait_callback:
            logger.info('The wait callback has been constructed, '
                        'the user is blocking on the response.')
            self.wait_callback()
        else:
            logger.info('The wait callback has not been constructed, ',
                        'the user has not called get() yet.')

    def timeout_callback(self):
        if not self.response_received:
            logger.error('The response timeout was called before the '
                         'response was received.')
            self.timed_out = True
            self.wait_callback()
        else:
            logger.info('The response timeout was called after the '
                        'response was received.')

class RPCResponse(object):
    def __init__(self, channel, method, header, body):
        self.channel = channel
        self.method = method
        self.header = header
        self.body_json = body

    _body = None
    @property
    def body(self):
        if not self._body:
            self._body = json.loads(self.body_json, use_decimal=True)
        return self._body

class AsyncSimpleConsumer(AsyncRabbitConnectionBase):
    @gen.engine
    def consume_queue(self, queue):
        yield gen.Task(self.basic_consume,
                       consumer_callback=self.consume_message,
                       queue=queue)

    def consume_message(self, channel, method, properties, body):
        raise NotImplemented('consume_message should be implemented '
                             'by subclasses of AsyncSimpleConsumer.')

class AsyncTornadoRPCClient(AsyncRabbitConnectionBase):

    """
    Wrap `pika.adapters.tornado_connection.TornadoConnection` 
    to provide a simple RPC client powered by ``tornado.gen.engine``
    semantics.

    """

    def __init__(self, *args, **kwargs):
        self.rpc_timeout = kwargs.pop('rpc_timeout', 5)

        self.declare_rpc_queue_lock = Lock()
        
        self.connection_open_callbacks = []
        self.rpc_queue_callbacks = []
        
        self.rpc_queue = None
        
        self.futures = {}

        super(AsyncTornadoRPCClient, self).__init__(*args, **kwargs)

    @gen.engine
    def rpc_queue_declare(self, callback, **kwargs):
        if not self.declare_rpc_queue_lock.acquire(False):
            logger.info('RPC Queue is already in the process of '
                        'being declared (declare_rpc_queue_lock '
                        'could not be acquired).')
            callback()
            return

        try:
            self.declaring_rpc_queue = True
            self.rpc_queue = yield gen.Task(self.queue_declare,
                                            exclusive=True)
            yield gen.Task(self.basic_consume, queue=self.rpc_queue)

            logger.info('Adding callbacks that are waiting for an RPC '
                        'queue to the tornado queue.')
            while self.rpc_queue_callbacks:
                cb = self.rpc_queue_callbacks.pop()
                self.io_loop.add_callback(cb)
            logger.info('Done adding callbacks.')
        finally:
            self.declare_rpc_queue_lock.release()

    @gen.engine
    def ensure_rpc_queue(self, callback):
        logger.info('Ensuring that an RPC queue has been declared.')
        yield Task(self.ensure_connection)
        if not self.rpc_queue:
            logger.info('Adding callback to list of callbacks '
                        'waiting for the RPC queue to be open.')
            self.rpc_queue_callbacks.append(callback)

            logger.info('Calling rpc_queue_declare().')
            rpc_queue = yield gen.Task(self.rpc_queue_declare)
            logger.info('rpc_queue_declare has been called.')
        else:
            logger.info('The RPC queue is already open.')
            callback()
    
    def rpc_timeout_callback(self, correlation_id):
        logger.info('RPC Client timeout callback called.')
        if correlation_id in self.futures:
            logger.warning('AsyncTornadoRPCClient.rpc_timeout_callback: '
                           'Future with correlation_id %s was still present '
                           'when the timeout was triggered.'
                           % correlation_id)
        self.futures.pop(correlation_id, None)

    @gen.engine
    def rpc(self, rpc_request, properties=None, callback=None):
        '''
        Publish an RPC request.  Returns a :class:`RPCResponseFuture`.

        :param rpc_request: An instance of :class:`RPCRequest`.

        '''

        yield Task(self.ensure_connection)
        yield Task(self.ensure_rpc_queue)
        
        if not properties:
            correlation_id = str(uuid.uuid4())
            properties = pika.BasicProperties(reply_to=self.rpc_queue,
                                              correlation_id=correlation_id)
        
        logger.info('Publishing RPC request with key: %s' %
                    rpc_request.routing_key)
        self.channel.basic_publish(exchange=rpc_request.exchange,
                                   routing_key=rpc_request.routing_key,
                                   body=rpc_request.json_params,
                                   properties=properties)

        logger.info('Constructing RPC response future with cid: %s' %
                    correlation_id)
        future = RPCResponseFuture(correlation_id,
                                   timeout=rpc_request.timeout,
                                   io_loop=self.io_loop)
        self.futures[correlation_id] = future

        timeout_callback = partial(self.rpc_timeout_callback, correlation_id)
        self.io_loop.add_timeout(timedelta(minutes=1),
                                 stack_context.wrap(timeout_callback))

        callback(future)

    @gen.engine
    def basic_publish(self, rpc_request, properties=None, callback=None):
        yield Task(self.ensure_connection)
        yield Task(self.ensure_rpc_queue)
        
        if not properties:
            properties = pika.BasicProperties()
        
        logger.info('Publishing message request with key: %s' %
                    rpc_request.routing_key)

        self.channel.basic_publish(exchange=rpc_request.exchange,
                                   routing_key=rpc_request.routing_key,
                                   body=rpc_request.json_params,
                                   properties=properties)
        logger.info('channel.basic_publish finished.')
        callback()
    
    def consume_message(self, channel, method, header, body):
        logger.info('RPC response consumed')
        cid = header.correlation_id

        response = RPCResponse(channel, method, header, body)
        try:
            future = self.futures.pop(cid)
            future.response_callback(response)
        except KeyError:
            logger.warning('AsyncRabbitClient.consume_message received an '
                           'unrecognized correlation_id: %s.  Maybe the '
                           'RPC took too long and was timed out, or maybe '
                           'the response was sent more than once.' % cid)

