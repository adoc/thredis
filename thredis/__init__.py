"""Just some simple threaded redis pool classes. Also some useful
primitive data models.

"""
# Date: 2014/03/19
# Author: https://github.com/adoc/
# © 2014 Nicholas Long. All Rights Reserved.

import logging
log = logging.getLogger(__name__)

import functools
import collections
import time
import threading
import uuid
import urllib.parse
import redis
import json as _json

from safedict import SafeDict


__all__ = ('JSONEncoder', 'JSONDecoder', 'json', 'dump_dict', 'load_dict', 'RedisPool',
            'ThreadLocalRedisPool', 'UnifiedSession', 'UnboundModelException', 'RedisObj',
            'Hash', 'Set', 'Collection')


# Let's monkeypatch json.dumps and json.loads to do what we want.
class Dummy:
    pass


class JSONEncoder(_json.JSONEncoder):
    """Simple encoder to handle UUID types. Converts to URN (Universal
    Resource Name); "urn:uuid:1111-..."
    
    """
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return obj.urn
        return _json.JSONEncoder.default(self, obj)


class JSONDecoder(_json.JSONDecoder):
    """Simple decoder to handle UUID types. Instances UUID object when
    value starts with "urn:uuid".

    """
    def decode(self, obj):
        obj = _json.JSONDecoder.decode(self, obj)
        # Handle potential UUID.
        if isinstance(obj, str) and obj.startswith('urn:uuid'):
            return uuid.UUID(obj)
        else:
            return obj


# Let's just make our own `json` object and overload the relevant funcs.
json = Dummy()
json.dumps = functools.partial(_json.dumps, separators=(',', ':'), cls=JSONEncoder)
json.loads = functools.partial(_json.loads, object_pairs_hook=collections.OrderedDict,
                                cls=JSONDecoder)


def dump_dict(obj):
    """Dump inner dictionary items. This is a preparation for
    insertion in to the string-valued Redis.

    """
    return {k: json.dumps(v) for k, v in obj.items()}


def load_dict(obj):
    """Load inner dictionary items. This is after retrieval from
    Redis.

    """
    return {k.decode(): json.loads(v.decode()) for k, v in obj.items()}


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
    """ """
    pipeline_commands = ('set', 'delete', 'hmset', 'zadd', 'zrem')
    client_commands = ('info', 'flushall', 'get', 'hgetall', 'zrange', 'zcard',
                        'zrangebyscore')

    def __init__(self, *args, **kwa):
        ThreadLocalRedisPool.__init__(self, *args, **kwa)
        self._exec_events = set()

    def __getattr__(self, attrname):
        if attrname in self.pipeline_commands:
            return getattr(self.pipeline, attrname)
        elif attrname in self.client_commands:
            return getattr(self.client, attrname)
        else:
            raise AttributeError("UnifiedSession has no attribute '%s'." % attrname)

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


class UnboundModelException(Exception):
    pass


class RedisObj:
    keyspace_separator = ':'

    def __init__(self, *namespace, session=None):
        if isinstance(session, UnifiedSession):
            self.__session = session
            self.__session.bind_exec_event(self._execute)
        elif session is None:
            self.__session = None
        else:
            raise ValueError("`RedisObj` session must be a `UnifiedSession`.")
        self.__namespace = namespace

    def _execute(self):
        """ """
        pass

    @property
    def session(self):
        # Yes, this is meant to check against None rather than
        #   isinstance. Notice the exception.
        if self.__session is None:
            raise UnboundModelException("This model is not bound to any session.")
        else:
            return self.__session
            self.__session.bind_exec_event(self._execute)
            
    @property
    def namespace(self):
        """ """
        return self.__namespace

    def bind(self, session):
        if isinstance(session, UnifiedSession):
            self.__session = session
        else:
            raise ValueError("`bind` requires a UnifiedSession object.")

    def gen_key(self):
        """ """
        return self.keyspace_separator.join(self.__namespace)

    def __del__(self):
        print("RedisObj was deleted!!!!")


class Hash(RedisObj):
    """


    >>> import thredis
    >>> 
    >>> s = thredis.UnifiedSession.from_url('redis://127.0.0.1:6379')
    >>> h = thredis.Hash('name', 'space', session=s)
    >>> 
    >>> id = h.set({'foo': True, 'bar': 'baz', 'boop': 123})
    >>> h.session.execute();
    [True]
    >>> 
    >>> h.get(id)
    {'boop': 123, 'foo': True, 'bar': 'baz', '_id': UUID('d12a97cc-46f2-460b-96c8-02ff18f55c95')}
    """
    def __init__(self, *namespace, random_id_func=uuid.uuid4, **kwa):
        """
        """
        if not len(namespace) > 0:
            raise ValueError('`Hash` requires at least one namespace argument.')
        RedisObj.__init__(self, *namespace, **kwa)
        self.__random_id_func = random_id_func

    def gen_key(self, _id):
        """
        """
        if isinstance(_id, bytes):
            _id = _id.decode()            
        return self.keyspace_separator.join([RedisObj.gen_key(self), str(_id)])

    def _get(self, _id):
        """Do the actual get operation.

        """
        return self.session.hgetall(Hash.gen_key(self, _id))

    def get(self, _id):
        """
        """
        return load_dict(Hash._get(self, _id))

    def _set(self, _id, obj):
        """Do the actual set operation.
        """
        return self.session.hmset(Hash.gen_key(self, _id), obj)

    def set(self, obj, **kwa):
        """
        """
        if 'id' not in kwa and '_id' not in obj:
            kwa['id'] = self.__random_id_func()
        elif 'id' not in kwa and '_id' in obj:
            kwa['id'] = obj['_id']

        if 'active' not in kwa and '_active' not in obj:
            kwa['active'] = True
        elif 'active' not in kwa and '_active' in obj:
            kwa['active'] = obj['_active']

        # Append _ to any additional items and extend in to obj.
        obj.update({'_%s' % k: v for k, v in kwa.items()})
        Hash._set(self, obj['_id'], dump_dict(obj))
        return obj['_id']

    def delete(self, _id, reference=True):
        """
        """
        if reference:
            obj = self.get(_id)
            Hash.set(self, obj, active=False)
        else:
            self.session.delete(Hash.gen_key(self, _id))

    def flush(self, _id):
        self.delete(_id, reference=False)


class Set(RedisObj):
    """

    >>> import thredis
    >>> s = thredis.UnifiedSession.from_url('redis://127.0.0.1:6379')
    >>> se = thredis.Set('set', 'space', session=s)
    >>> 
    >>> se.add('set1')
    >>> se.add('set2')
    >>> se.add('set3')
    >>> 
    >>> se.session.execute()
    [1, 1, 1]
    >>> 
    >>> se.all()
    [b'set1', b'set2', b'set3']
    >>> 
    >>> se.insert('set1.5', 1)
    >>> 
    >>> se.session.execute()
    [1, 0, 0]
    >>> 
    >>> se.all()
    [b'set1', b'set1.5', b'set2', b'set3']
    >>> 
    """
    def __init__(self, *args, **kwa):
        RedisObj.__init__(self, *args, **kwa)
        self.__dirty_count = 0

    def _execute(self):
        """ """
        self.__dirty_count = 0

    def count(self):
        return self.session.zcard(self.gen_key()) + self.__dirty_count

    def add(self, obj, score=None):
        if not isinstance(score, float):
            score = self.count()
        self.session.zadd(self.gen_key(), score, obj)
        self.__dirty_count += 1

    def range(self, from_idx, to_idx):
        return self.session.zrange(self.gen_key(), int(from_idx), int(to_idx))

    def get(self, idx):
        item = self.range(idx, idx)
        if len(item) > 0:
            return item[0]

    def all(self):
        return self.range(0, -1)

    def delete(self, obj):
        self.session.zrem(self.gen_key(), obj)
        self.__dirty_count -= 1

    def flush(self):
        # TODO: Rename any other "flush" methods that aren't directly
        #   related to removing keys or data.
        """This will completely remove this sets namespace from Redis.

        """
        self.session.delete(self.gen_key())

    def insert(self, obj, at_idx):
        """Insert object `at_index`, reindexing all objects following."""
        idx = float(at_idx)
        self.session.execute() # Unfortunate hack to keep the indexing correct.
        self.add(obj, score=idx)
        for item in self.range(idx, -1):
            idx += 1
            self.add(item, score=idx)
           

class Collection(RedisObj):
    """Provides a hash and ordered set.

    >>> import thredis
    >>> 
    >>> s = thredis.UnifiedSession.from_url('redis://127.0.0.1:6379')
    >>> s.flushall()
    True
    >>> c = thredis.Collection('coll', 'space', session=s)
    >>> 
    >>> id = c.add({'foo': 'bars'})
    >>> 
    >>> c.session.execute()
    [True, 1]
    >>> 
    >>> c.get(id)
    {'foo': 'bars', '_idx': 0, '_id': UUID('3ff956ae-d36b-416b-8c83-9103a0ebc7ff')}
    >>> 
    >>> c.add({'foo': 'bars'})

    >>> c.add({'foo': 'bars'})

    >>> c.add({'foo': 'bars'})

    >>> c.session.execute()
    [True, 1, True, 1, True, 1]
    >>> list(c.all(active_only=False))

    """
    def __init__(self, *namespace, **kwa):
        RedisObj.__init__(self, *namespace, **kwa)
        self.__hash = Hash(*namespace+('data',), **kwa)
        self.__index = Set(*namespace+('idx',), **kwa)
        self.__active_index = Set(*namespace+('active',), **kwa)

    def all(self, active_only=True):
        """Get all items in the collection and return as a generator.
        """
        if active_only is True:
            id_set = self.__active_index.all()
        else:
            id_set = self.__index.all()
        return (self.__hash.get(_id) for _id in id_set)

    def all_list(self, **kwa):
        return list(self.all(**kwa))

    def idx(self, _id, verify=True):
        obj = self.__hash.get(_id)
        
        if not verify or self.__index.get(obj['_idx']) == obj['_id']:
            return obj['_idx']
        else:
            # TODO: Custom exception for this. A bad one.
            raise Exception("Inconsitency in Redis. This is bad and should never happen.")

    def move(self):
        pass

    def get(self, _id):
        return self.__hash.get(_id)

    def add(self, obj, **kwa):
        if 'idx' not in kwa:
            kwa['idx'] = self.__index.count()
        # Do hash add and then put in to set before item at `idx`.
        _id = self.__hash.set(obj, **kwa)
        self.__index.insert(_id, kwa['idx'])
        return _id

    def flush(self):
        """Removes this entire collection from Redis."""
        self.__active_index.flush()
        for _id in self.__index:
            self.__hash.flush(_id)
        self.__index.flush()

    def delete(self):
        pass