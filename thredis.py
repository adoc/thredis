"""Just some simple threaded redis pool classes."""

from __future__ import absolute_import

import logging
log = logging.getLogger(__name__)

import thread
import urlparse
import redis


class RedisPool(object):
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
        if not isinstance(url, urlparse.ParseResult):
            url = urlparse.urlparse(url)

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


class ThreadLocalRedis(RedisPool):
    """
    A persistent Redis implementation that provides thread local
    clients and pipelines. A single instance of this class is meant to
    be shared to multiple threads.

    """
    __clients = {}
    __pipelines = {}

    def __init__(self, thread_ident_func=thread.get_ident, **kwa):
        """

        """
        RedisPool.__init__(self, **kwa)
        self.__thread_ident_func = thread_ident_func
        
    @property
    def _thread_id(self):
        # Executes the thread id function and returns the result.
        return self.__thread_ident_func()

    @property
    def client(self):
        """Returns a thread local Redis Client."""
        thread_id = self._thread_id
        client = self.__clients[thread_id] = \
            self.__clients.get(thread_id,
                                self.get_client())
        return client

    @property
    def pipeline(self):
        """Returns a thread local Redis Pipeline."""
        thread_id = self._thread_id
        pipeline = self.__pipelines[thread_id] = \
            self.__pipelines.get(thread_id,
                                    self.client.pipeline())
        return pipeline

    def remove_pipeline(self, thread_id=None):
        """Remove the thread local Redis Pipeline."""
        thread_id = thread_id or self._thread_id
        if thread_id in self.__pipelines:
            log.debug("%s: Removing Redis Pipeline for thread: %s." %
                        (self.__class__.__name__, thread_id))
            del self.__pipelines[thread_id]        

    def remove_client(self, thread_id=None):
        """Remove the thread local Redis Pipeline and Client."""
        thread_id = thread_id or self._thread_id
        self.remove_pipeline(thread_id=thread_id)
        if thread_id in self.__clients:
            log.debug("%s: Removing Redis Client for thread: %s." %
                        (self.__class__.__name__, thread_id))
            del self.__clients[thread_id]

    def remove(self):
        """Alias to `remove_client`."""
        self.remove_client(thread_id=self._thread_id)


# Sample implementation.

class NotificationsRedisCache(ThreadLocalRedis):
    """API to Redis backend for the Notifications class. Implements Redis
    Sorted Sets with the Unix Timestamp as the Score.

    """
    def add(self, key, score, data):
        """Add Notification item to set using a Redis pipeline."""

        log.debug("%s: Adding member to Redis Sorted Set (%s, %s, %s)." %
                    (self.__class__.__name__, key, score, data))
        self.pipeline.zadd(key, score, data)

    def count(self, key):
        """ """
        return self.client.zcard(key)

    def get(self, key):
        """Get all items in the sorted set."""
        log.debug("%s: Retrieving Redis Sorted Sets for key (%s)." %
                    (self.__class__.__name__, key))
        return self.client.zrange(key, 0, -1, withscores=True)

    def get_by_score(self, key, score):
        """Get all items in the sorted set by `score`."""
        log.debug("%s: Retrieving Redis Sorted Sets for key, score (%s, %s)." %
                    (self.__class__.__name__, key, score))
        return self.client.zrangebyscore(key, score, score, withscores=True)

    def delete(self, key, *members, **kwa):
        """Delete `members` from sorted set of `key`. Alternately if
        `delete_key` is True then delete the entire key"""
        if kwa.get('delete_key'):
            log.debug("%s: Deleting Redis key (%s)." %
                        (self.__class__.__name__, key))
            self.pipeline.delete(key)
        elif members:
            log.debug("%s: Deleting member(s) of Redis Sorted Set (%s, %s)." %
                        (self.__class__.__name__, key, members))
            self.pipeline.zrem(key, *members)
        else:
            raise AttributeError("Nothing was done. No `members` specified and `delete_all` set to False.")

    def execute(self):
        """Execute the thread local Redis pipeline"""
        log.debug("%s: Executing Redis Pipeline (%s)." %
                    (self.__class__.__name__, self._thread_id))
        return self.pipeline.execute()