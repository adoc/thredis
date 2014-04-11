"""Just some simple threaded redis pool classes. Also some useful
primitive data models.

"""
# Date: 2014/03/19
# Author: https://github.com/adoc/
# © 2014 Nicholas Long. All Rights Reserved.

import logging
log = logging.getLogger(__name__)

import functools
import inspect
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
            'String', 'List', 'Set', 'ZSet', 'Hash')


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

class HashableOrderedDict(collections.OrderedDict):
    """
    *** This hash does not support nested dicts.
    """
    def __hash__(self):
        return hash(frozenset(self))


# Let's just make our own `json` object and overload the relevant funcs.
json = Dummy()
json.dumps = functools.partial(_json.dumps,
                                separators=(',', ':'),
                                cls=JSONEncoder)
json.loads = functools.partial(_json.loads,
                                object_pairs_hook=HashableOrderedDict,
                                cls=JSONDecoder)

#json.loads = functools.partial(_json.loads, object_pairs_hook=collections.OrderedDict,
#                                cls=JSONDecoder)


def dump_dict(obj):
    """Dump inner dictionary items. This is a preparation for
    insertion in to the string-valued Redis.

    """
    return {k: json.dumps(v) for k, v in obj.items() if v}


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
    pipeline_commands = (
                            # All keys
                            'delete',
                            # String
                            'set',
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
                        # Strings
                        'get',
                        # Lists
                        'lrem', 'lindex', 'llen', 'lrange',
                        'lpop',
                        'rpop',
                        # Hashes
                        'hgetall',
                        # Sets
                        'smembers', 'scard', 'sismember',
                        # Sorted Sets
                        'zrange', 'zcard', 'zrangebyscore')

    # REMOVE THIS!
    client_commands += ('flushall', 'info')

    def __init__(self, *args, **kwa):
        ThreadLocalRedisPool.__init__(self, *args, **kwa)
        self._exec_events = set()

    def __getattr__(self, attrname):
        if attrname in self.pipeline_commands:
            return getattr(self.pipeline, attrname)
        elif attrname in self.client_commands:
            return getattr(self.client, attrname)
        else:
            raise AttributeError("UnifiedSession has no attribute '%s'." %
                                    attrname)

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

def decorate(decorator):
    """Will decorate a class or a single function.
    """
    #
    def _decorator(decorated):

        if inspect.isclass(decorated):
            target = decorated.__dict__.copy()

            for k, v in target.items():
                if callable(v) and k.startswith("r_"):
                    setattr(decorated, k.lstrip('r_'), decorator(v))
            return decorated
        elif callable(decorated):
            return decorator(decorated)
        else:
            raise ValueError()

    return _decorator



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
        if not len(namespace) > 0:
            raise ValueError('`RedisObj` requires at least one namespace argument.')
        self.__namespace = namespace
        log.debug("RedisObj instantiated. namespace: %s" % (':'.join(self.__namespace)))

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
            
    @property
    def namespace(self):
        """ """
        return self.__namespace

    def bind(self, session):
        if isinstance(session, UnifiedSession):
            self.__session = session
        else:
            raise ValueError("`bind` requires a UnifiedSession object.")

    def gen_key(self, *raw_suffix):
        """Handles generating a keyspace for this object.
        """
        suffix = []
        # Not using list comprehension because we exect to add more
        # type casting in here.
        for name in raw_suffix:
            if isinstance(name, bytes):
                name = name.decode()
            else:
                name = str(name)
            suffix.append(name)
        # Concat the namespace and the suffix and join with separator.
        return self.keyspace_separator.join(self.__namespace +
                                                tuple(suffix))

    def __del__(self):
        pass

    # General model actions
    def flush(self, *keyspace):
        key = self.gen_key(*keyspace)
        return self.session.delete(key)

    # Utilities
    @staticmethod
    def ingress(val):
        """Ingress data."""
        logging.debug(val)
        if isinstance(val, bytes):
            return json.loads(val.decode())

        elif isinstance(val, list):
            return [json.loads(v.decode()) for v in val]

        elif isinstance(val, set):
            return set([json.loads(v.decode()) for v in val])

        elif isinstance(val, dict):
            return load_dict(val)

        elif val:
            return json.loads(val)

    @staticmethod
    def egress(*args):
        return tuple([json.dumps(val) for val in args])


    @staticmethod
    def egress_hash(**obj):
        return dump_dict(obj)


class String(RedisObj):
    """
    """
    # Low functions
    def r_get(self):
        key = self.gen_key()
        logging.debug("""Low level string get key %s.""" % key)
        return self.session.get(key)

    def r_set(self, val):
        key = self.gen_key()
        logging.debug("""Low level string set key %s val %s.""" % (key, val))
        return self.session.set(key, val)

    # API functions
    def get(self):
        return self.ingress(self.r_get())

    def set(self, val):
        return self.r_set(*self.egress(val))

    def delete(self):
        key = self.gen_key()
        return self.session.delete(key)


class List(RedisObj):
    """
    """

    # Low functions
    def r_range(self, from_idx, to_idx):
        key = self.gen_key()
        return self.session.lrange(key, from_idx, to_idx)

    def r_lpush(self, *objs):
        key = self.gen_key()
        return self.session.lpush(key, *objs)

    def r_rpush(self, *objs):
        key = self.gen_key()
        return self.session.rpush(key, *objs)

    def r_set(self, idx, obj):
        key = self.gen_key()
        return self.session.lset(key, idx, obj)

    def r_lpop(self):
        key = self.gen_key()
        return self.session.lpop(key)

    def r_rpop(self):
        key = self.gen_key()
        return self.session.rpop(key)


    # API functions
    def all(self):
        """Get all in list."""
        return self.ingress(self.r_range(0, -1))

    def count(self):
        """Return list count/length."""
        key = self.gen_key()
        return self.session.llen(key)

    def lpush(self, *objs):
        """Insert obj(s) at start of list."""
        return self.r_lpush(*self.egress(*objs))

    def rpush(self, *objs):
        """Insert obj(s) at end of list."""
        return self.r_rpush(*self.egress(*objs))

    def set(self, idx, obj):
        """Set list item at `idx` to `obj`."""
        return self.r_set(idx, *self.egress(obj))

    def get(self, idx):
        """Get list item at `idx`."""
        return self.ingress(self.r_range(idx, idx))

    def lpop(self):
        """Get first list item and remove it."""
        return self.ingress(self.r_lpop())

    def rpop(self):
        """Get last list item and remove it."""
        return self.ingress(self.r_rpop())


class Set(RedisObj):
    """
    """

    def r_all(self):
        key = self.gen_key()
        return self.session.smembers(key)

    def r_add(self, *objs):
        key = self.gen_key()
        return self.session.sadd(key, *objs)

    def r_ismember(self, obj):
        key = self.gen_key()
        return self.session.sismember(key, obj)

    def r_delete(self, *objs):
        key = self.gen_key()
        return self.session.srem(key, *objs)


    def count(self):
        key = self.gen_key()
        return self.session.scard(key)

    def all(self):
        return self.ingress(self.r_all())

    def add(self, *objs):
        return self.r_add(*self.egress(*objs))

    def ismember(self, obj):
        return self.r_ismember(*self.egress(obj))

    def delete(self, *objs):
        return self.r_delete(*self.egress(*objs))


class ZSet(RedisObj):
    """
    """

    reindex_threshold = 0.1

    def r_range(self, from_idx, to_idx, reversed_=False, withscores=False):
        key = self.gen_key()
        zrange = (reversed_ and self.session.zrevrange or
                                self.session.zrange)
        return zrange(key, int(from_idx), int(to_idx), withscores=withscores)

    def r_all(self, reversed_=False, withscores=False):
        key = self.gen_key()
        return self.r_range(0, -1, reversed_, withscores)

    def r_get(self, idx):
        key = self.gen_key()
        return self.r_range(key, idx, idx)

    def r_add(self, obj):
        score = 0.0
        key = self.gen_key()
        
        last = self.session.zrange(key, -1, -1, withscores=True) # :(

        if last:
            score = last[0][1] + 1.0
        return self.session.zadd(key, score, obj)

    def r_between(self, low_idx, high_idx, obj):
        key = self.gen_key()
        left_score = 0.0
        right_score = 0.0

        lbound = self.session.zrange(key, low_idx, low_idx, withscores=True)
        rbound = self.session.zrange(key, high_idx, high_idx, withscores=True)

        if lbound:
            left_score = lbound[0][1]

        if rbound:
            right_score = rbound[0][1]

        print(left_score, right_score)

        target_score = (left_score + right_score) / 2
        variance = right_score - target_score

        if variance < self.reindex_threshold:
            ret = self.session.client.zadd(key, target_score, obj)
            logging.info("Redis ZSet '%s' is reindexing. Variance (%s) fell "
                "below threshold of (%s)." % (key, variance,
                                                self.reindex_threshold))
            self.reindex()
            return ret
        else:
            return self.session.zadd(key, target_score, obj)

    def r_delete(self, obj):
        key = self.gen_key()
        return self.session.zrem(key, obj)


    # Api functions
    def reindex(self):
        idx = 0.0
        key = self.gen_key()

        for item in self.r_all():
            self.session.zadd(key, idx, item)
            idx += 1.0
        return idx

    def count(self):
        key = self.gen_key()
        return self.session.zcard(key)

    def range(self, from_idx, to_idx, reversed=False):
        return self.ingress(self.r_range(from_idx, to_idx, reversed))

    def all(self, reversed=False):
        return self.ingress(self.r_range(0,-1, reversed))

    def get(self, idx):
        return self.ingress(self.r_get(idx))

    def add(self, obj):
        return self.r_add(*self.egress(obj))

    def between(self, low_idx, high_idx, obj):
        return self.r_between(low_idx, high_idx, *self.egress(obj))

    def insert(self, idx, obj):
        return self.r_between(idx, idx-1, *self.egress(obj))

    def delete(self, obj):
        return self.r_delete(*self.egress(obj))


class Hash(RedisObj):
    """
    """
    
    # Low functions
    def r_get(self, key):
        key = self.gen_key(key)
        return self.session.hgetall(key)

    def r_set(self, key, obj):
        key = self.gen_key(key)
        return self.session.hmset(key, obj)

    # API functions
    def get(self, key):
        return self.ingress(self.r_get(key))

    def set(self, key, obj):
        return self.r_set(key, self.egress_hash(**obj))

    def delete(self, key):
        key = self.gen_key(key)
        return self.session.delete(key)








'''


class Set(RedisObj):
    def __init__(self, *args, **kwa):
        RedisObj.__init__(self, *args, **kwa)

    def count(self):
        return self.session.scard(self.gen_key())

    def add(self, *obj):
        self.session.sadd(self.gen_key(), *obj)

    def ismember(self, obj):
        return self.session.sismember(self.gen_key(), obj)

    def all(self):
        return self.session.smembers(self.gen_key())

    def delete(self, *obj):
        self.session.srem(self.gen_key(), *obj)






# Kind of an extended "Hash" object.
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
    id_attr = 'id'
    internal_keys = {'active', 'idx'}
    internal_defaults = {'active': True}

    def __init__(self, *namespace, random_id_func=uuid.uuid4, **kwa):
        """
        """
        RedisObj.__init__(self, *namespace, **kwa)
        self.__random_id_func = random_id_func

    @property
    def _all_internal_keys(self):
        return self.internal_keys | {self.id_attr}

    def _get(self, _id):
        """Do the actual get operation.

        """
        return self.session.hgetall(Hash.gen_key(self, _id))

    def _set(self, _id, obj):
        """Do the actual set operation.
        """
        return self.session.hmset(Hash.gen_key(self, _id), obj)

    def gen_key(self, _id):
        """Handles generating a key for a given hash `id`.
        """
        # TODO: Let's verify this is a clean solution.
        if isinstance(_id, bytes):
            _id = _id.decode()
        return self.keyspace_separator.join([RedisObj.gen_key(self), str(_id)])

    def get(self, _id, with_internal=True):
        """
        """
        obj = load_dict(Hash._get(self, _id))

        # prepend any "internal" keyspaces with '_'
        for key in list(self._all_internal_keys):
            if key in obj:
                if with_internal is True:
                    obj['_'+key] = obj[key]
                del obj[key]
        return obj

        # obj['id'] = str(obj['_id'])
        # del obj['_id']
        # del obj['_idx']
        # return obj

    def set(self, obj, **kwa):
        """
        keyword args express internals that should be set. (id, active, etc.)
        """
        # What is the distinction between obj and kwa?
        # Obj accepts internal keyspaces if _ prepended.
        # Kwa accepts internal keyspaces only?

        set_obj = {}

        # Set of internal keys in kwa
        kwa_ikeys = self._all_internal_keys.intersection(kwa.keys())
        # Set of internal keys in obj. Since obj contains '_' prepended internal
        # keys, this comprehension is a bit nested. There might be a shorter
        # way.
        obj_ikeys = {k.lstrip('_') for k in
                        {'_'+k for k in
                            self._all_internal_keys}.intersection(obj.keys())}

        all_ikeys = kwa_ikeys | obj_ikeys

        # Set default id.
        if self.id_attr not in all_ikeys:
            set_obj[self.id_attr] = self.__random_id_func()

        # Set defaults.
        for k, v in self.internal_defaults.items():
            if k not in all_ikeys:
                set_obj[k] = v



        # if 'id' not in kwa and '_id' not in obj:
        #     kwa['id'] = self.__random_id_func()
        # elif 'id' not in kwa and '_id' in obj:
        #     kwa['id'] = obj['_id']

        # if 'active' not in kwa and '_active' not in obj:
        #     kwa['active'] = True
        # elif 'active' not in kwa and '_active' in obj:
        #     kwa['active'] = obj['_active']

        # Append _ to any additional items and extend in to obj.
        # obj.update({'_%s' % k: v for k, v in kwa.items()})
        # Hash._set(self, obj['_id'], dump_dict(obj))
        # return obj['_id']


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





class ZSet(RedisObj):
    """

    >>> import thredis
    >>> s = thredis.UnifiedSession.from_url('redis://127.0.0.1:6379')
    >>> se = thredis.ZSet('set', 'space', session=s)
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
        super(ZSet, self)._execute()
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
    id_attr = 'id'
    idx_attr = 'idx'

    def __init__(self, *namespace, **kwa):
        RedisObj.__init__(self, *namespace, **kwa)
        self.__hash = Hash(*namespace+('h', 'data'), **kwa)
        self.__index = ZSet(*namespace+('z', 'index'), **kwa)
        self.__inactive_index = Set(*namespace+('s', 'deleted'), **kwa)

    def all_gen(self, inactive=False):
        """Get all items in the collection and return as a generator.
        """
        if inactive is True:
             id_set = self.__inactive_index.all()
        else:
            id_set = self.__index.all()
        return (self.__hash.get(_id) for _id in id_set)

    def all(self, **kwa):
        return list(self.all_gen(**kwa))

    def idx(self, _id, verify=True):
        obj = self.__hash.get(_id)
        
        if not verify or self.__index.get(obj['_idx']) == obj['_id']:
            return obj['_idx']
        else:
            # TODO: Custom exception for this. A bad one.
            raise Exception("Inconsitency in Redis. This is bad and should never happen.")

    def move(self):
        raise NotImplementedError()

    def get(self, _id):
        return self.__hash.get(_id)

    def add(self, obj, **kwa):

        if 'idx' not in kwa:
            kwa['idx'] = self.__index.count()
        # Do hash add and then put in to set before item at `idx`.
        _id = self.__hash.set(obj, **kwa)
        
        if obj['_active'] is True:
            self.__index.insert(_id, kwa['idx'])
        elif obj['_active'] is False:
            self.__inactive_index.add(_id)
        else:
            raise ValueError("`active` is not set correctly." % obj['_active'])

        return {'_id': obj['_id']}

    def flush(self):
        """Removes this entire collection from Redis."""
        self.__active_index.flush()
        for _id in self.__index:
            self.__hash.flush(_id)
        self.__index.flush()

    def delete(self, _id, **kwa):
        print('model delete.')
        if kwa.get('reference', True) is True:
            self.__index.delete(_id)
            self.__hash.delete(_id, **kwa)
            self.__inactive_index.add(_id)
            return True
        else:
            raise NotImplementedError()


            '''