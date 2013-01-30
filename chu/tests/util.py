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

from functools import partial
from Cookie import SimpleCookie
import uuid
import numbers
from datetime import timedelta

from tornado import gen
from tornado.ioloop import IOLoop
from tornado import stack_context


def gen_wrapper(*args, **kwargs):
    from nose.tools import make_decorator
    from tornado import gen

    timeout = None

    def get_wrapper(f):
        @make_decorator(f)
        def wrapper(self, *args, **kwargs):
            bound_f = partial(gen.engine(f), self, *args, **kwargs)
            self.io_loop.add_callback(stack_context.wrap(bound_f))
            if timeout:
                self.wait(timeout=timeout)
            else:
                self.wait()
        return wrapper

    if 'timeout' in kwargs:
        timeout = kwargs['timeout']
        return get_wrapper
    else:
        return get_wrapper(args[0])


class AsyncEvent(object):
    def __init__(self, timeout=None, io_loop=None):
        self.is_set = False
        self.timeout = timeout
        self.wait_callbacks = {}
        self.timeout_set = set()
        self.io_loop = io_loop
        if not self.io_loop:
            self.io_loop = IOLoop.instance()

    def set(self):
        self.is_set = True
        for cb_key in self.wait_callbacks.keys():
            self.io_loop.add_callback(self.wait_callbacks.pop(cb_key))

    @gen.engine
    def wait(self, callback, timeout=None):
        if self.is_set:
            callback(True)
            return

        if self.timeout and not timeout:
            timeout = self.timeout

        wait_key = uuid.uuid4()
        wait_cb = yield gen.Callback(wait_key)
        self.wait_callbacks[wait_key] = stack_context.wrap(wait_cb)

        if timeout:
            if isinstance(timeout, numbers.Real):
                timeout = timedelta(seconds=timeout)
            timeout_cb = partial(self.timeout_callback, wait_key)
            self.io_loop.add_timeout(timeout, stack_context.wrap(timeout_cb))

        yield gen.Wait(wait_key)

        callback(self.is_set)

    def timeout_callback(self, key):
        if self.is_set:
            return
        self.timeout_set.add(key)
        self.io_loop.add_callback(self.wait_callbacks.pop(key))
