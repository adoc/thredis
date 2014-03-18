"""Just some simple threaded redis pool classes."""

from __future__ import absolute_import

import logging
log = logging.getLogger(__name__)

import threading
import urllib.parse
import redis

from safedict import SafeDict


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


class RedisCommandsMix(object):
    """
    This mixin unifies client and pipeline commands on a RedisPool
    instance.

    Some commands are pipelined and some are instant. We might want
    this to put everything in to the pipeline, even gets.
    """
    @staticmethod
    def zadd(*args, **kwa):
        return lambda self: self.pipeline.zadd(*args, **kwa)

    @staticmethod
    def zrange(*args, **kwa):
        return lambda self: self.client.zrange(*args, **kwa)

    @staticmethod
    def zcard(*args, **kwa):
        return lambda self: self.client.zcard(*args, **kwa)

    @staticmethod
    def zrangebyscore(*args, **kwa):
        return lambda self: self.client.zrangebyscore(*args, **kwa)

    @staticmethod
    def zrem(*args, **kwa):
        return lambda self: self.pipeline.zrem(*args, **kwa)

    @staticmethod
    def get(*args, **kwa):
        return lambda self: self.client.get(*args, **kwa)

    @staticmethod
    def set(*args, **kwa):
        return lambda self: self.pipeline.set(*args, **kwa)

    @staticmethod
    def delete(*args, **kwa):
        return lambda self: self.pipeline.delete(*args, **kwa)


class RedisSession:
    """ """
    __registry = SafeDict(threading.get_ident)

    def __init__(self, pool, commandset=RedisCommandsMix):
        self._pool = pool
        self._commandset = RedisCommandsMix

    @property
    def queue(self):
        try:
            queue = self.__registry['queue']
        except KeyError:
            queue = self.__registry['queue'] = []
        return queue

    def remove_queue(self):
        if 'queue' in self.__registry:
            del self.__registry['queue']

    def _transact(self, transaction):
        return transaction(self._pool)

    def set(self, *args, **kwa):
        self.queue.append(self._commandset.set(*args, **kwa))

    def get(self, *args, **kwa):
        return self._transact(self._commandset.get(*args, **kwa))

    def delete(self, *args, **kwa):
        self.queue.append(self._commandset.delete(*args, **kwa))

    def zadd(self, *args, **kwa):
        self.queue.append(self._commandset.zadd(*args, **kwa))

    def zrange(self, *args, **kwa):
        return self._transact(self._commandset.zrange(*args, **kwa))

    def zcard(self, *args, **kwa):
        return self._transact(self._commandset.zcard(*args, **kwa))

    def zrangebyscore(self, *args, **kwa):
        return self._transact(self._commandset.zrangebyscore(*args, **kwa))

    def zrem(self, *args, **kwa):
        self.queue.append(self._commandset.zrem(*args, **kwa))

    def flush(self):
        results = [self._transact(transaction) for transaction in self.queue]
        self.remove_queue()
        return results

    def commit(self):
        self.flush()
        try:
            self._pool.pipeline.execute()
        finally:
            self._pool.remove_pipeline()