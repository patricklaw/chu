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

import tornado.ioloop
import tornado.web
import tornado.gen
from datetime import timedelta
import chu.rpc

@tornado.gen.engine
def simple_rpc():
    # Make sure you have rabbitmq running locally, or modify localhost
    client = chu.rpc.AsyncTornadoRPCClient(host='localhost')

    # The params passed to RPCRequest are serialized to JSON
    rpc_request = chu.rpc.RPCRequest(exchange='exch',
                                     routing_key='key',
                                     params=dict(ncc=1701))

    # client.rpc returns a future
    future = yield tornado.gen.Task(client.rpc, rpc_request)

    try:
        # We yield the future's get method as a Task to wait for the response
        response = yield tornado.gen.Task(future.get,
                                          timeout=timedelta(seconds=6))
    except chu.rpc.RPCTimeoutError as e:
        print "Oops, we didn't get a response"

    # clean up the IOLoop, since this is a singleton example
    tornado.ioloop.IOLoop.instance().add_callback(io_loop.stop)

if __name__ == "__main__":
    io_loop = tornado.ioloop.IOLoop.instance()
    io_loop.add_callback(simple_rpc)
    io_loop.add_timeout(timedelta(seconds=10), io_loop.stop)
    io_loop.start()
