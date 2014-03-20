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
            'Hash', 'TestGeneralFunctions', 'TestRedisPoolClass', 'TestThreadLocalRedisPool',
            'TestUnifiedSession', 'TestUnboundModelException', 'TestRedisObject')


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

    def __getattr__(self, attrname):
        if attrname in self.pipeline_commands:
            return getattr(self.pipeline, attrname)
        elif attrname in self.client_commands:
            return getattr(self.client, attrname)
        else:
            raise AttributeError("UnifiedSession has no attribute '%s'." % attrname)

    def execute(self):
        try:
            return self.pipeline.execute()
        finally:
            self.remove_pipeline()


class UnboundModelException(Exception):
    pass


class RedisObj:
    keyspace_separator = ':'

    def __init__(self, *namespace, session=None):
        if isinstance(session, UnifiedSession):
            self.__session = session
        elif session is None:
            self.__session = None
        else:
            raise ValueError("`RedisObj` session must be a `UnifiedSession`.")
        self.__namespace = namespace

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

    def gen_key(self):
        """ """
        return self.keyspace_separator.join(self.__namespace)

    def __del__(self):
        print("RedisObj was deleted!!!!")


class Hash(RedisObj):
    """
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
        return self.keyspace_separator.join([RedisObj.gen_key(self), str(_id)])

    def redis_get(self, _id):
        """Do the actual get operation.

        """
        return self.session.hgetall(Hash.gen_key(self, _id))

    def get(self, _id):
        """
        """
        return load_dict(Hash.redis_get(self, _id))

    def redis_set(self, _id, obj):
        """Do the actual set operation.
        """
        return self.session.hmset(Hash.gen_key(self, _id), obj)

    def set(self, obj, **kwa):
        """
        """
        if 'id' not in kwa and '_id' not in obj:
            kwa['id'] = self.__random_id_func()
        elif '_id' in obj:
            kwa['id'] = obj['_id']

        # Append _ to any additional items and extend in to obj.
        obj.update({'_%s' % k: v for k, v in kwa.items()})
        Hash.redis_set(self, obj['_id'], dump_dict(obj))
        return obj['_id']

    def delete(self, _id, reference=True):
        """
        """
        if reference:
            obj = self.get(_id)
            Hash.set(self, obj, active=False)
        else:
            self.session.delete(Hash.gen_key(self, _id))


class Set(RedisObj):
    """ """
    def count(self):
        return self.session.zcard(self.gen_key())

    def add(self, obj, score=None):
        if not isinstance(score, float):
            idx = self.count()
        self.session.zadd(self.gen_key(), idx, obj)

    def range(self, from_idx, to_idx):
        return self.session.zrange(self.gen_key(), from_idx, to_idx)

    def get(self, idx):
        return self.range(idx, idx)

    def all(self):
        return self.range(0, -1)

    def delete(self, obj):
        self.session.zrem(self.gen_key(), obj)

    def insert(self, obj, at_idx):
        """Insert object `at_index`, reindexing all objects following."""
        idx = float(at_idx)
        self.add(obj, score=idx)
        for item in self.range(idx, -1):
            idx += 1
            self.add(item, score=idx)
            




class Collection(RedisObj):
    """Provides a hash and ordered set.

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
        return (Hash.get(_id) for _id in id_set)
        

    def idx(self, _id, verify=True):
        obj = Hash.get(self, _id)
        
        if not verify or Set.get(self, obj['_idx']) == obj['_id']:
            return obj['_idx']
        else:
            # TODO: Custom exception for this. A bad one.
            raise Exception("Inconsitency in Redis. This is bad and should never happen.")

    def move(self):
        pass

    def get(self, _id):
        return Hash.get(self, _id)

    def add(self, obj, **kwa):
        if 'idx' in kwa:
            # Do hash add and then put in to set before item at `idx`.
            pass
        else:
            idx = Set.count(self)

        _id = Hash.set(self, obj, idx=idx)

    def delete(self):
        pass



class Collection(Hash, Set):
    """Provides a hash and ordered set.

    """
    def __init__(self, *namespace, **kwa):
        Hash.__init__(self, *namespace, **kwa)
        Set.__init__(self, *namespace, **kwa)

    @property
    def keyspace(self):
        return self._keyspace

    def gen_hash_key(self, id_):
        """Generate hash key."""
        return self._hash_key_tmpl.format(keyspace=self.keyspace,
                                            id=id_)

    def gen_set_key(self):
        """Generate set key."""
        return self._set_key_tmpl.format(keyspace=self.keyspace)

    def list(self, active_only=True):
        """Ordered list of elements in the collection. This returns a generator."""
        # This is a generator.
        collection_ids = self.session.zrange(self.gen_set_key(), 0, -1)
        return (self.session.hgetall(self.gen_hash_key(id_)) for id_ in collection_ids)

    def idx(self, id, verify=True):
        """Return the index position of the given id."""
        obj = self.session.hmget(self.gen_hash_key(id_))
        idx_ = obj['_idx']
        if not verify or self.session.zrange(self.gen_set_key(), idx_, idx_) == obj['_id']:
            return obj['_idx']
        else:
            raise Exception("Inconsitency in Redis. This is bad and should never happen.")

    def move(self, id, to_idx):
        pass

    def get(self, id_):
        """ """
        return self._load_dict(self.session.hgetall(self.gen_hash_key(id_)))

    def add(self, obj, active=True):
        """Add object to the collection. Returns the id of the new object."""
        set_key = self.gen_set_key()
        idx_ = self.session.zcard(set_key) # Value of last index + 1
        obj = isinstance(obj, dict) and obj or obj.__dict__
        id_ = self._random_id_func()
        obj['_active'] = active
        obj['_id'] = id_
        obj['_idx'] = idx_
        # Set hash and record position in sorted set.
        self.session.hmset(self.gen_hash_key(id_), self._dump_dict(obj))
        self.session.zadd(set_key, idx_, id_)
        return id_
        
    def delete(self, id_, reference=True):
        """ """
        set_key = self.gen_set_key()
        self.session.zrem(set_key, id_)
        obj = self.get(id_)



# TESTS
import unittest
import threading

threadfunc = lambda func: threading.Thread(target=func)

class RedisTestCase(unittest.TestCase):
    """Set up the redis connection information here. `url` is used in
    most tests, so that should match what's in `host`,`port`,`db`,`password`.

    Password in url is accomplished using normal syntax and -anything-
    for "user" as it is ignored.

    redis://ignored_username:realpassword12345@myredis.local:6380/1

    """
    host = "localhost"
    port = 6379
    db = 0
    password = None
    url = "redis://127.0.0.1:6379/0"

    namespace = "test"


class TestGeneralFunctions(unittest.TestCase):
    """Test general functions and objects.
    (Dummy, JSONEncoder, JSONDecoder, dump_dict, load_dict)

    """
    def test_dummy(self):
        d = Dummy()
        d.foo = 'bar'
        d.bar = 'baz'
        self.assertIs(d.foo, 'bar')
        self.assertIs(d.bar, 'baz')

    def test_json_encoder(self):
        je = JSONEncoder()
        _id = uuid.uuid4()
        self.assertEqual(je.default(_id), 'urn:uuid:' + str(_id))

    def test_json_decoder(self):
        jd = JSONDecoder()
        self.assertEqual(jd.decode('"urn:uuid:b6ea2918-e2f1-4c0a-aa50-948edb9120fa"'),
                            uuid.UUID('b6ea2918-e2f1-4c0a-aa50-948edb9120fa'))
        self.assertEqual(jd.decode('"foo"'), 'foo')
        self.assertEqual(jd.decode('true'), True)
        self.assertEqual(jd.decode('false'), False)
        self.assertEqual(jd.decode('123'), 123)
        self.assertEqual(jd.decode('123.123'), 123.123)

    def test_dump_dict(self):
        d = {'foo': 'bar', 'boo': True, 'boop': False, 'baz': 123.123}
        expect = {'boo': 'true', 'baz': '123.123', 'foo': '"bar"', 'boop': 'false'}
        self.assertEqual(dump_dict(d), expect)

    def test_load_dict(self):
        l = {b'boo': b'true', b'baz': b'123.123', b'foo': b'"bar"', b'boop': b'false'}
        expect = {'foo': 'bar', 'boo': True, 'boop': False, 'baz': 123.123}
        self.assertEqual(load_dict(l), expect)


class TestRedisPoolClass(RedisTestCase):
    """Test RedisPool class"""
    def _connect(self):
        return RedisPool(host=self.host, port=self.port, db=self.db,
                        password=self.password)

    def test_init(self, pool=None):
        pool = pool or self._connect()
        self.assertIs(pool._RedisPool__client_cls, redis.StrictRedis)
        self.assertIsInstance(pool._RedisPool__pool, redis.ConnectionPool)

    def test_get_client(self, pool=None):
        pool = pool or self._connect()
        client = pool.get_client()
        info = client.info()
        self.assertEqual(info['tcp_port'], self.port)

    def test_from_url(self):
        pool = RedisPool.from_url(self.url)
        self.test_init(pool)
        self.test_get_client(pool)


class TestThreadLocalRedisPool(RedisTestCase):
    def _connect(self):
        return ThreadLocalRedisPool.from_url(self.url)

    def test_init(self, pool=None):
        pool = pool or self._connect()
        TestRedisPoolClass.test_init(self, pool)
        self.assertIsInstance(pool._ThreadLocalRedisPool__registry, SafeDict)

    def test_client(self, pool=None):
        pool = pool or self._connect()

        def check():
            ident = threading.get_ident()
            self.assertIs(pool.client,
                pool._ThreadLocalRedisPool__registry['client'][ident])

        threads = (threadfunc(check),
                    threadfunc(check),
                    threadfunc(check))
        
        def start(t):
            t.start()
            t.join()

        map(start, threads)

    def test_pipeline(self, pool=None):
        pool = pool or self._connect()

        def check():
            ident = threading.get_ident()
            self.assertIs(pool.pipeline,
                pool._ThreadLocalRedisPool__registry['pipeline'][ident])

        threads = (threadfunc(check),
                    threadfunc(check),
                    threadfunc(check))
        
        def start(t):
            t.start()
            t.join()

        map(start, threads)

    def test_remove_pipeline(self, pool=None):
        pool = pool or self._connect()

        def check():
            ident = threading.get_ident()
            self.assertIs(pool.pipeline,
                pool._ThreadLocalRedisPool__registry['pipeline'][ident])

            pool.remove_pipeline()
            self.assertRaises(KeyError, pool._ThreadLocalRedisPool__registry['pipeline'][ident])

        threads = (threadfunc(check),
                    threadfunc(check),
                    threadfunc(check))
        
        def start(t):
            t.start()
            t.join()

        map(start, threads)

    def test_remove_client(self, pool=None):
        pool = pool or self._connect()

        def check():
            ident = threading.get_ident()
            self.assertIs(pool.client,
                pool._ThreadLocalRedisPool__registry['client'][ident])

            pool.remove_pipeline()
            self.assertRaises(KeyError, pool._ThreadLocalRedisPool__registry['client'][ident])

        threads = (threadfunc(check),
                    threadfunc(check),
                    threadfunc(check))
        
        def start(t):
            t.start()
            t.join()

        map(start, threads)

    def test_remove(self, pool=None):
        pool = pool or self._connect()
        self.assertEqual(pool.remove, pool.remove_client)


class TestUnifiedSession(RedisTestCase):
    """ """
    def _connect(self):
        return UnifiedSession.from_url(self.url)

    def test_getattr(self, session=None):
        session = session or self._connect()
        for cmd in session.pipeline_commands:
            self.assertEqual(getattr(session.pipeline, cmd), getattr(session, cmd))

        for cmd in session.client_commands:
            self.assertEqual(getattr(session.client, cmd), getattr(session, cmd))

    def test_execute(self, session=None):
        session = session or self._connect()

        # No clue how to test the execute other than to use the
        #   commands on keys in the `test_namespace`
        self.assertTrue(False, 'No assertions for this test.')


class TestUnboundModelException(unittest.TestCase):
    """Simply test that the exception is an exception. Once it logs or
    does other things, we will add more tests.

    """
    def test_exception(self):
        def do_raise():
            raise UnboundModelException()
        assert issubclass(UnboundModelException, Exception)
        self.assertRaises(UnboundModelException, do_raise)


class TestRedisObject(RedisTestCase):
    """ """
    def _connect(self):
        return UnifiedSession.from_url(self.url)

    def _redisobj(self, session):
        return RedisObj(self.namespace, session=session)

    def test_init(self):
        r = RedisObj('namespace1', 'namespace2')

        self.assertEqual(r._RedisObj__namespace, ('namespace1', 'namespace2'))
        self.assertIsNone(r._RedisObj__session)

        s = self._connect()
        r = RedisObj('namespace1', 'namespace2', session=s)

        self.assertEqual(r._RedisObj__namespace, ('namespace1', 'namespace2'))
        self.assertIs(r._RedisObj__session, s)

    def test_session(self):
        r = self._redisobj(None)

        self.assertRaises(UnboundModelException, lambda: r.session)

        s = self._connect()
        r = self._redisobj(s)

        self.assertIs(r.session, s)

    def test_namespace(self):
        r = RedisObj('namespace!')
        self.assertEqual(r.namespace, ('namespace!',))

    def test_bind(self):
        r = self._redisobj(None)

        self.assertRaises(ValueError, lambda: r.bind(None))

        s = self._connect()
        r.bind(s)

        self.assertIs(r.session, s)

    def test_gen_key(self):
        r = RedisObj('ns1', 'ns2', 'ns3')
        self.assertEqual(r.gen_key(), 'ns1:ns2:ns3')


class TestHash(RedisTestCase):
    pass