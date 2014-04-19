""" """
import pprint

import unittest
import time
import uuid
import thredis
import thredis.model
import thredis.util


class TestRedisObj(unittest.TestCase):
    """
    """
    test_sequence = 0
    test_namespace = 'test:RedisObj:'+str(uuid.uuid1(test_sequence,
                                    clock_seq=int(time.time()*1000)))

    # manually egressed data
    test_string_payload = thredis.util.json.dumps(str(uuid.uuid4()))
    test_list_payload = [thredis.util.json.dumps(str(uuid.uuid4())) for _ in range(10)]
    test_dict_payload = {str(uuid.uuid4()):thredis.util.json.dumps(str(uuid.uuid4()))
                            for _ in range(10)}
    test_set_payload = {thredis.util.json.dumps(str(uuid.uuid4())) for _ in range(10)}

    def setUp(self):
        self.session = thredis.UnifiedSession.from_url(
                                                'redis://127.0.0.1:6379/10')
        self.client = self.session.client

        #Setup some data for delete test.
        self.assertIs(
            self.client.set(self.test_namespace+':delete1',
                                    self.test_string_payload),
            True)
        self.assertIs(
            self.client.set(self.test_namespace+':delete2',
                                    self.test_string_payload),
            True)

        #Setup some data for ingress test.
        self.assertIs(
            self.client.set(self.test_namespace+':ingress_string',
                                    self.test_string_payload),
            True)
        self.assertIs(
            self.client.rpush(self.test_namespace+':ingress_list',
                                 *self.test_list_payload),
            10)
        self.assertIs(
            self.client.hmset(self.test_namespace+':ingress_dict',
                                self.test_dict_payload),
            True)
        self.assertIs(
            self.client.sadd(self.test_namespace+':ingress_set',
                                *self.test_set_payload),
            10)

    def tearDown(self):
        """Flush the test db."""
        self.session.client.flushdb()
        self.assertEqual(len(self.session.client.keys()), 0)

    def test_construction(self):
        self.assertEqual(thredis.model.RedisObj.keyspace_separator, ':')

        # Empty construct
        self.assertRaises(ValueError, thredis.model.RedisObj)

        # Namespace
        obj = thredis.model.RedisObj(self.test_namespace)
        self.assertEqual(obj._RedisObj__namespace, (self.test_namespace,))
        
        obj = thredis.model.RedisObj(self.test_namespace, '123')
        self.assertEqual(obj._RedisObj__namespace, (self.test_namespace,'123'))

        # Session
        self.assertRaises(ValueError,
                lambda: thredis.model.RedisObj(self.test_namespace, session=123))
        
        obj = thredis.model.RedisObj(self.test_namespace)
        self.assertIs(obj._RedisObj__session, None)
        
        obj = thredis.model.RedisObj(self.test_namespace, session=self.session)
        self.assertIs(obj._RedisObj__session, self.session)

    def test_prop_session(self):
        obj = thredis.model.RedisObj(self.test_namespace)
        self.assertRaises(thredis.model.UnboundModelException, lambda: obj.session)

        obj = thredis.model.RedisObj(self.test_namespace, session=self.session)
        self.assertIs(obj.session, self.session)

        def setter(): obj.session = self.session
        self.assertRaises(AttributeError, setter)
        self.assertIs(obj.session, self.session)

    def test_prop_namespace(self):
        obj = thredis.model.RedisObj(self.test_namespace)
        self.assertEqual(obj.namespace, (self.test_namespace,))

        obj = thredis.model.RedisObj(self.test_namespace)

        def setter(): obj.namespace = ('namespace', 'is', 'this')
        self.assertRaises(AttributeError, setter)
        self.assertEqual(obj.namespace, (self.test_namespace,))

    def test_meth_bind(self):
        obj = thredis.model.RedisObj(self.test_namespace)
        self.assertRaises(ValueError, lambda: obj.bind(None))
        self.assertRaises(ValueError, lambda: obj.bind(123))

        obj.bind(self.session)
        self.assertIs(obj._RedisObj__session, self.session)

    def test_meth_gen_key(self):
        sep = thredis.model.RedisObj.keyspace_separator
        obj = thredis.model.RedisObj(self.test_namespace)
        self.assertEqual(obj.gen_key(), self.test_namespace)
        obj = thredis.model.RedisObj(self.test_namespace, 'this', 'now')
        self.assertEqual(obj.gen_key(), sep.join((self.test_namespace, 'this', 'now')))
        self.assertEqual(obj.gen_key('ok', 'then'),
                            sep.join((self.test_namespace,'this','now','ok','then')))

    def test_meth_delete(self):
        obj = thredis.model.RedisObj(self.test_namespace, session=self.session)

        obj.delete('delete1')
        self.session.execute()
        self.assertEqual(self.session.client.get(
            self.test_namespace+':delete1'), None)

        obj.delete('delete1')
        self.session.execute()
        self.assertEqual(self.session.client.get(
            self.test_namespace+':delete1'), None)

        print(self.session.client.keys())

    def test_static__ingress(self):
        ingress = thredis.model.RedisObj._ingress

        self.assertEqual(
            ingress(self.session.client.get(self.test_namespace+':ingress_string')),
            thredis.util.json.loads(self.test_string_payload)
            )

        self.assertEqual(
            ingress(self.session.client.lrange(self.test_namespace+':ingress_list', 0, -1)),
            [thredis.util.json.loads(val) for val in self.test_list_payload]
            )

        self.assertEqual(
            ingress(self.session.client.hgetall(self.test_namespace+':ingress_dict')),
            {key: thredis.util.json.loads(val) for key, val in self.test_dict_payload.items()}
            )

        self.assertEqual(
            ingress(self.session.client.smembers(self.test_namespace+':ingress_set')),
            {thredis.util.json.loads(val) for val in self.test_set_payload}
            )

    def test_static__egress(self):
        egress = thredis.model.RedisObj._egress

        self.assertEqual(egress(1,2,3), ('1','2','3'))


class TestString(unittest.TestCase):
    test_sequence = 0
    test_namespace = 'test:String:'+str(uuid.uuid1(test_sequence,
                                    clock_seq=int(time.time()*1000)))

    # manually egressed data
    test_string_payload = str(uuid.uuid4())

    def setUp(self):
        self.session = thredis.UnifiedSession.from_url(
                                                'redis://127.0.0.1:6379/10')
        self.client = self.session.client

        #Setup some data for delete test.
        self.assertIs(
            self.client.set(self.test_namespace+':delete1',
                                    self.test_string_payload),
            True)
        self.assertIs(
            self.client.set(self.test_namespace+':delete2',
                                    self.test_string_payload),
            True)
        self.assertIs(
            self.client.set(self.test_namespace+':string1',
                                    self.test_string_payload),
            True)
        self.assertIs(
            self.client.set(self.test_namespace+':json_string1',
                                    thredis.util.json.dumps(self.test_string_payload)),
            True)

    def tearDown(self):
        """Flush the test db."""
        self.client.flushdb()
        self.assertEqual(len(self.client.keys()), 0)

    def test_r_get(self):
        obj = thredis.model.String(self.test_namespace, 'string1',
                                        session=self.session)
        self.assertEqual(obj.r_get().decode(), self.test_string_payload)

    def test_r_set(self):
        obj = thredis.model.String(self.test_namespace, 'string2',
                                        session=self.session)
        self.assertEqual(obj.r_set(self.test_string_payload).execute(),
                [True])

    def test_get(self):
        obj = thredis.model.String(self.test_namespace, 'json_string1',
                                        session=self.session)
        self.assertEqual(obj.get(), self.test_string_payload)

    def test_set(self):
        obj = thredis.model.String(self.test_namespace, 'string2',
                                        session=self.session)
        self.assertEqual(obj.set(self.test_string_payload).execute(), [True])

    def test_delete(self):
        obj = thredis.model.String(self.test_namespace, 'delete1',
                                        session=self.session)
        obj.delete().execute()
        self.assertIs(obj.get(), None)

        obj = thredis.model.String(self.test_namespace, 'delete2',
                                        session=self.session)
        obj.delete().execute()
        self.assertIs(obj.get(), None)


class TestList(unittest.TestCase):
    test_sequence = 0
    test_namespace = 'test:List:'+str(uuid.uuid1(test_sequence,
                                    clock_seq=int(time.time()*1000)))

    ingressed_list_payload = [uuid.uuid4() for _ in range(10)]
    egressed_list_payload = [thredis.util.json.dumps(str(val)).encode() 
                                for val in ingressed_list_payload]

    def setUp(self):
        self.session = thredis.UnifiedSession.from_url(
                                                'redis://127.0.0.1:6379/10')
        self.client = self.session.client

        self.assertIs(
            self.client.rpush(self.test_namespace+":list", *self.egressed_list_payload),
            10)

    def tearDown(self):
        """Flush the test db."""
        self.client.flushdb()
        self.assertEqual(len(self.client.keys()), 0)

    def test_r_range(self):
        obj = thredis.model.List(self.test_namespace, 'list',
                                    session=self.session)
        self.assertEqual(obj.r_range(0, -1), self.egressed_list_payload)
        self.assertEqual(obj.r_range(1, -2), self.egressed_list_payload[1:-1])
        self.assertEqual(obj.r_range(2, -2), self.egressed_list_payload[2:-1])

    def test_r_lpush(self):
        obj = thredis.model.List(self.test_namespace, 'list',
                                    session=self.session)
        u = [thredis.util.json.dumps(str(uuid.uuid4())).encode()
                for _ in range(5)]
        obj.r_lpush(*u).execute()

        pprint.pprint(u)
        pprint.pprint(self.client.lrange(self.test_namespace+':list', 0, len(u)))

        self.assertEqual(
            self.client.lrange(self.test_namespace+':list', 0, len(u)-1),
            list(reversed(u)))

    def test_r_rpush(self):
        obj = thredis.model.List(self.test_namespace, 'list',
                                    session=self.session)
        u = [thredis.util.json.dumps(str(uuid.uuid4())).encode()
                for _ in range(5)]
        obj.r_lpush(*u).execute()

        pprint.pprint(u)
        pprint.pprint(self.client.lrange(self.test_namespace+':list', 0, len(u)))

        self.assertEqual(
            self.client.lrange(self.test_namespace+':list', 0, len(u)-1),
            list(reversed(u)))

    def test_r_set(self):
        pass

    def test_r_lpop(self):
        pass

    def test_r_rpop(self):
        pass

    def test_all(self):
        pass

    def test_count(self):
        pass

    def test_lpush(self):
        pass

    def test_rpush(self):
        pass

    def test_get(self):
        pass

    def test_lpop(self):
        pass

    def test_rpop(self):
        pass