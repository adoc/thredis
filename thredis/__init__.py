"""Just some simple threaded redis pool classes. Also some useful
primitive data models.

"""
# Creted: 2014/03/19
# Version: 0.3
# Date: 2014/04/11
# Author: https://github.com/adoc/
# © 2014 Nicholas Long. All Rights Reserved.

import logging
log = logging.getLogger(__name__)

import functools
import inspect
import pprint
import collections
import time
import threading
import uuid
import urllib.parse
import redis
import json as _json

from safedict import SafeDict


__all__ = ('RedisPool', 'ThreadLocalRedisPool', 'UnifiedSession', )


class RedisPool:
    """
    A Redis implementation that utilizes a process persistent
    ConnectionPool.

    """
    def __init__(self, host='localhost', port=6379, db=0, password=None,
                 max_connections=None, client_cls=redis.StrictRedis,
                 pool_cls=redis.ConnectionPool, **kwa):
        """Construct a new RedisPool instance.

        """
        kwa.update({
            'host': host,
            'port': port,
            'db': db,
            'password': password,
            'max_connections': max_connections
            })

        self.__client_cls = client_cls

        log.info("%s: Setting up the Redis Connection Pool." %
                    self.__class__.__name__)
        log.debug("%s: Pool connection parameters %s" %
                    (self.__class__.__name__, kwa))
        self.__pool = pool_cls(**kwa)

    @classmethod
    def from_url(cls, url, db=None, **kwa):
        """Contstruct a new ThreadLocalRedis instance using a url for
        the connection parameters. Similar to StrictRedis.from_url().

        Can accept a urlparse.ParseResult or a string.

        """ 
        if not isinstance(url, urllib.parse.ParseResult):
            url = urllib.parse.urlparse(url)

        assert url.scheme == 'redis' or not url.scheme

        if db is None:
            try:
                db = int(url.path.lstrip('/'))
            except ValueError:
                db = 0
        return cls(host=url.hostname, port=url.port, db=db,
                    password=url.password, **kwa)

    def get_client(self):
        """Retrieve a Redis Client with a Connection from the pool."""
        return self.__client_cls(connection_pool=self.__pool)


class ThreadLocalRedisPool(RedisPool):
    """
    A persistent Redis implementation that provides thread local
    clients and pipelines. A single instance of this class is meant to
    be shared to multiple threads.

    """
    __registry = SafeDict(threading.get_ident)

    @property
    def client(self):
        """Returns a thread local Redis Client."""
        try:
            client = self.__registry['client']
        except KeyError:
            client = self.__registry['client'] = self.get_client()

        return client

    @property
    def pipeline(self):
        """Returns a thread local Redis Pipeline."""
        try:
            pipeline = self.__registry['pipeline']
        except KeyError:
            pipeline = self.__registry['pipeline'] = self.client.pipeline()

        return pipeline

    def remove_pipeline(self):
        """Remove the thread local Redis Pipeline."""

        if 'pipeline' in self.__registry:
            log.debug("%s: Removing Redis Pipeline for thread: %s." %
                        (self.__class__.__name__, self.__registry.thread_id))
            del self.__registry['pipeline']

    def remove_client(self):
        """Remove the thread local Redis Pipeline and Client."""

        if 'client' in self.__registry:
            log.debug("%s: Removing Redis Client for thread: %s." %
                        (self.__class__.__name__, self.__registry.thread_id))
            self.remove_pipeline()
            del self.__registry['client']

    remove = remove_client


class UnifiedSession(ThreadLocalRedisPool):
    """Session to unify Redis command calling. Various commands are
    either pipelined or executed immediately.


    * More are easily added 
    Pipelined Commands:
      All keys:
        delete
      String:
        set
      Lists:
        lset, linsert, lpush, rpush
      Hashes:
        hmset
      Sets:
        sadd, srem
      Sorted Sets:
        zadd, zrem

    Immediate Commands:
      Strings:
        get
      Lists:
        lrem, lindex, llen, lrange, lpop, rpop
      Hashes:
        hgetall, hmget
      Sets
        smembers, scard, sismember
      Sorted Sets
        zrange, zrevrange, zcard, zrangebyscore

    """

    # TODO: Better facilities to execute pipelines, etc.

    # Command whitelisting and unification.
    # These commands will be available on the session object but
    # All commands can still be accessed via `client` and `pipeline`
    # attributes.
    pipeline_commands = (
                            # All keys
                            'delete',
                            # String
                            'set', 'incrby', 'pexpire',
                            # Lists
                            'lset', 'linsert', 'lpush',
                            'rpush',
                            # Hashes
                            'hmset',
                            # Sets
                            'sadd', 'srem',
                            # Sorted Sets
                            'zadd', 'zrem')

    client_commands = (
                        'keys', 'register_script',
                        # Strings
                        'get',
                        # Lists
                        'lrem', 'lindex', 'llen', 'lrange',
                        'lpop', 'blpop',
                        'rpop',
                        # Hashes
                        'hgetall', 'hmget',
                        # Sets
                        'smembers', 'scard', 'sismember',
                        # Sorted Sets
                        'zcard', 'zscore',
                        'zrange', 'zrevrange',
                        'zrangebyscore', 'zrevrangebyscore')

    debug_commands = ('flushdb', 'info')

    def __init__(self, *args, **kwa):
        # Remove debug commands if not expressly in debug mode.
        # This prevents accidental nasties.
        if kwa.get('debug') is not True:
            self.debug_commands = ()

        self.__do_pipeline = kwa.get('pipeline', True)

        ThreadLocalRedisPool.__init__(self, *args, **kwa)
        self._exec_events = set()

    def __getattr__(self, attrname):
        if attrname in self.pipeline_commands:
            return getattr(self.pipeline, attrname)
        elif attrname in self.client_commands+self.debug_commands:
            return getattr(self.client, attrname)
        else:
            raise AttributeError("UnifiedSession has no attribute '%s'." %
                                    attrname)

    def delete_wild(self, wildkey):
        """ A quick shortcut to be able to delete wildcard namespaces.
        Use responsibly!"""
        keys = self.keys(wildkey)
        self.delete(*keys)

    def pp_keys(self, *args, sort=True):
        """Pretty print the keys."""
        keys = self.keys(*args)
        if sort is True:
            keys.sort()
        pprint.pprint(keys)

    def bind_exec_event(self, callback):
        self._exec_events.add(callback)

    def unbind_exec_event(self, callback):
        self._exec_events.discard(callback)

    def trigger_exec_event(self):
        for callback in self._exec_events:
            callback()

    def execute(self):
        try:
            return self.pipeline.execute()
        finally:
            self.trigger_exec_event()
            self.remove_pipeline()