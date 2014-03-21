""" """

import uuid
import unittest
import threading
import redis
from safedict import SafeDict
from thredis import *
from thredis import Dummy

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

    def _session(self):
        return UnifiedSession.from_url(self.url)


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


class TestUnboundModelException(unittest.TestCase):
    """Simply test that the exception is an exception. Once it logs or
    does other things, we will add more tests.

    """
    def test_exception(self):
        def do_raise():
            raise UnboundModelException()
        assert issubclass(UnboundModelException, Exception)
        self.assertRaises(UnboundModelException, do_raise)


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
    def test_getattr(self, session=None):
        session = session or self._session()
        for cmd in session.pipeline_commands:
            self.assertEqual(getattr(session.pipeline, cmd), getattr(session, cmd))

        for cmd in session.client_commands:
            self.assertEqual(getattr(session.client, cmd), getattr(session, cmd))

    def test_execute(self, session=None):
        session = session or self._session()

        # No clue how to test the execute other than to use the
        #   commands on keys in the `test_namespace`
        self.assertTrue(False, 'No assertions for this test.')


class TestRedisObject(RedisTestCase):
    """ """
    def _redisobj(self, session):
        return RedisObj(self.namespace, session=session)

    def test_init(self):
        r = RedisObj('namespace1', 'namespace2')

        self.assertEqual(r._RedisObj__namespace, ('namespace1', 'namespace2'))
        self.assertIsNone(r._RedisObj__session)

        s = self._session()
        r = RedisObj('namespace1', 'namespace2', session=s)

        self.assertEqual(r._RedisObj__namespace, ('namespace1', 'namespace2'))
        self.assertIs(r._RedisObj__session, s)

    def test_session(self):
        r = self._redisobj(None)

        self.assertRaises(UnboundModelException, lambda: r.session)

        s = self._session()
        r = self._redisobj(s)

        self.assertIs(r.session, s)

    def test_namespace(self):
        r = RedisObj('namespace!')
        self.assertEqual(r.namespace, ('namespace!',))

    def test_bind(self):
        r = self._redisobj(None)

        self.assertRaises(ValueError, lambda: r.bind(None))

        s = self._session()
        r.bind(s)

        self.assertIs(r.session, s)

    def test_gen_key(self):
        r = RedisObj('ns1', 'ns2', 'ns3')
        self.assertEqual(r.gen_key(), 'ns1:ns2:ns3')


class TestHash(RedisTestCase):
    # TODO: Make sure to test both bytes and strings.
    def _hash(self, session):
        return Hash(self.namespace, 'hash', session=session)

    def setUp(self):
        session = self._session()
        session.pipeline.hmset(self.namespace+':hash:1',
                                {'_id': '1',
                                    'foo': '"bartest!"'})
        session.pipeline.hmset(self.namespace+':hash:2',
                                {'_id': '2',
                                    'foo': '"bartest!"',
                                    'bar': 'true',
                                    'baz': '123.123'})
        session.pipeline.execute()

    def test_init(self):
        h = Hash('hash', 'space')
        self.assertEqual(h.namespace, ('hash', 'space'))
        self.assertIs(h._Hash__random_id_func, uuid.uuid4)

    def test_gen_key(self):
        h = Hash('hash', 'space')
        self.assertEqual(h.gen_key('id123'), 'hash:space:id123')

    def test__get(self):
        h = self._hash(self._session())
        self.assertEqual(h._get('1'), {b'_id': b'1',
                                        b'foo': b'"bartest!"'})
        self.assertEqual(h._get('2'), {b'_id': b'2',
                                        b'foo': b'"bartest!"',
                                        b'bar': b'true',
                                        b'baz': b'123.123'})

    def test_get(self):
        h = self._hash(self._session())
        self.assertEqual(h.get('1'), {'_id': 1,
                                        'foo': 'bartest!'})
        self.assertEqual(h.get('2'), {'_id': 2,
                                        'foo': 'bartest!',
                                        'bar': True,
                                        'baz': 123.123})

    def test__set(self):
        s = self._session()
        h = self._hash(s)
        h._set('3', {'foo': '"bartest!"',
                        'bar': 'true',
                        'baz': '123.123'})
        s.pipeline.execute()
        self.assertEqual(s.client.hgetall(self.namespace+':hash:3'),
                            {b'foo': b'"bartest!"',
                                b'bar': b'true',
                                b'baz': b'123.123'})

    def test_set(self):
        s = self._session()
        h = self._hash(s)
        _id = h.set({'foo': 'bartest!',
                'bar': True,
                'baz': 123.123})
        s.pipeline.execute()
        self.assertEqual(s.client.hgetall(self.namespace+':hash:'+str(_id)),
                            {b'_id': b'"'+_id.urn.encode()+b'"',
                                b'_active': b'true',
                                b'foo': b'"bartest!"',
                                b'bar': b'true',
                                b'baz': b'123.123'})

    def test_delete(self):
        s = self._session()
        h = self._hash(s)
        s.pipeline.hmset(self.namespace+':hash:delete1',
                                {'_id': '"delete1"',
                                    '_active': 'true',
                                    'foo': '"bartest!"'})
        s.pipeline.hmset(self.namespace+':hash:delete2',
                                {'_id': '"delete2"',
                                    '_active': 'true',
                                    'foo': '"bartest!"',
                                    'bar': 'true',
                                    'baz': '123.123'})

        self.assertIsNotNone(s.client.hgetall(self.namespace+':hash:delete1'))
        self.assertIsNotNone(s.client.hgetall(self.namespace+':hash:delete2'))        
        h.delete('delete1')
        h.delete('delete2', reference=False)
        s.pipeline.execute()
        self.assertEqual(s.client.hgetall(self.namespace+':hash:delete1')[b'_active'], b'false')
        self.assertEqual(s.client.hgetall(self.namespace+':hash:delete2'), {})

        
class TestSet(RedisTestCase):
    """"""
    def _set(self, session, space='1'):
        return Set(self.namespace, 'set', space, session=session)

    def setUp(self):
        session = self._session()
        session.client.delete(self.namespace+':set:1')
        session.client.delete(self.namespace+':set:2')
        session.client.delete(self.namespace+':set:3')
        session.client.delete(self.namespace+':set:4')
        session.client.delete(self.namespace+':set:5')
        session.client.delete(self.namespace+':set:6')
        session.client.delete(self.namespace+':set:7')
        session.pipeline.zadd(self.namespace+':set:1', 1.0, 'foobar')
        session.pipeline.zadd(self.namespace+':set:1', 2.0, 'boobaz')
        session.pipeline.zadd(self.namespace+':set:1', 3.0, 'booper')
        session.pipeline.execute()

    def test_count(self):
        s = self._set(self._session())
        self.assertIs(s.count(), 3)

    def test_add(self):
        session = self._session()
        
        s = self._set(session, space='2')
        s.add('addtest1')
        s.add('addtest2')
        s.add('addtest1.5', score=0.5)
        session.execute()
        
        self.assertEqual(
            session.client.zrange(self.namespace+':set:2', 0, -1),
                [b'addtest1', b'addtest1.5', b'addtest2'])

    def test_range(self):
        session = self._session()

        session.pipeline.zadd(self.namespace+':set:3', 1.0, 'rangetest1')
        session.pipeline.zadd(self.namespace+':set:3', 2.0, 'rangetest2')
        session.pipeline.zadd(self.namespace+':set:3', 3.0, 'rangetest3')
        session.pipeline.zadd(self.namespace+':set:3', 4.0, 'rangetest4')
        session.pipeline.zadd(self.namespace+':set:3', 5.0, 'rangetest5')
        session.pipeline.execute()

        s = self._set(session, space='3')
        self.assertEqual(s.range(0, -1), [b'rangetest1', b'rangetest2', b'rangetest3',
                                            b'rangetest4', b'rangetest5'])
        self.assertEqual(s.range(1,-2), [b'rangetest2', b'rangetest3',
                                            b'rangetest4'])

    def test_get(self):
        session = self._session()

        session.pipeline.zadd(self.namespace+':set:4', 1.0, 'gettest1')
        session.pipeline.zadd(self.namespace+':set:4', 2.0, 'gettest2')
        session.pipeline.zadd(self.namespace+':set:4', 3.0, 'gettest3')
        session.pipeline.zadd(self.namespace+':set:4', 4.0, 'gettest4')
        session.pipeline.zadd(self.namespace+':set:4', 5.0, 'gettest5')
        session.pipeline.execute()

        s = self._set(session, space='4')
        self.assertEqual(s.get(0), b'gettest1')
        self.assertEqual(s.get(2), b'gettest3')
        self.assertEqual(s.get(-1), b'gettest5')


    def test_all(self):
        session = self._session()

        session.pipeline.zadd(self.namespace+':set:5', 1.0, 'alltest1')
        session.pipeline.zadd(self.namespace+':set:5', 2.0, 'alltest2')
        session.pipeline.zadd(self.namespace+':set:5', 3.0, 'alltest3')
        session.pipeline.zadd(self.namespace+':set:5', 4.0, 'alltest4')
        session.pipeline.zadd(self.namespace+':set:5', 5.0, 'alltest5')
        session.pipeline.execute()

        s = self._set(session, space='5')

        self.assertEqual(s.all(), [b'alltest1', b'alltest2', b'alltest3', b'alltest4', b'alltest5'])

    def test_delete(self):
        session = self._session()

        session.pipeline.zadd(self.namespace+':set:6', 1.0, 'deletetest1')
        session.pipeline.zadd(self.namespace+':set:6', 2.0, 'deletetest2')
        session.pipeline.zadd(self.namespace+':set:6', 3.0, 'deletetest3')
        session.pipeline.zadd(self.namespace+':set:6', 4.0, 'deletetest4')
        session.pipeline.zadd(self.namespace+':set:6', 5.0, 'deletetest5')
        session.pipeline.execute()

        s = self._set(session, space='6')

        s.delete('deletetest4')
        s.delete('deletetest2')
        s.session.execute()

        self.assertEqual(session.zrange(self.namespace+':set:6', 0, -1),
                [b'deletetest1', b'deletetest3', b'deletetest5'])

    def test_insert(self):
        session = self._session()

        session.pipeline.zadd(self.namespace+':set:7', 1.0, 'deletetest1')
        session.pipeline.zadd(self.namespace+':set:7', 3.0, 'deletetest3')
        session.pipeline.zadd(self.namespace+':set:7', 5.0, 'deletetest5')
        session.pipeline.execute()

        s = self._set(session, space='7')

        s.insert('deletetest2', 1)
        s.insert('deletetest4', 3)

        s.session.execute()

        self.assertEqual(session.zrange(self.namespace+':set:7', 0, -1),
                [b'deletetest1', b'deletetest2', b'deletetest3',  b'deletetest4', b'deletetest5'])


class TestCollection(RedisTestCase):
    def _collection(self, session):
        return Collection(self.namespace, 'collection', session=session)

    def test_init(self):
        c = Collection('test', 'collection')

        self.assertIsInstance(c._Collection__hash, Hash)
        self.assertIsInstance(c._Collection__index, Set)
        self.assertIsInstance(c._Collection__active_index, Set)

    def test_all(self):
        pass

    def test_idx(self):
        pass

    def test_move(self):
        pass

    def test_get(self):
        pass

    def test_add(self):
        pass

    def test_delete(self):
        pass
