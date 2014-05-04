""" """
import logging
log = logging.getLogger(__name__)


import threading
import base64
import time
import uuid
import thredis
import thredis.util

from safedict import SafeDict

__all__ = ('UnboundModelException', 'ConstraintFailed', 'UniqueFailed',
            'RedisObj',
            'String', 'List', 'Set', 'ZSet', 'Hash',
            'ModelObject', 'Record', 'Nonces')

def timedec(func):
    def innerfunc(*args, **kwa):
        t0=time.time()
        try:
            return func(*args,  **kwa)
        finally:
            name = func.__name__
            print("func `%s` timing: %s" % (name, time.time()-t0))
    return innerfunc



class UnboundModelException(Exception):
    """Exception thrown when a `session` is requested on an unbound
    model."""
    pass


class ConstraintFailed(Exception):
    pass


class UniqueFailed(ConstraintFailed):
    pass


class PassThrough:
    @staticmethod
    def _ingress(obj):
        return obj
    @staticmethod
    def _egress(*obj):
        return obj


# TODO: Move these originally in "model" to "primal"
class RedisObj:
    keyspace_separator = ':'

    # copied to util.lua
    l_copy = """
    -- Atomic Redis Copy.
    local source_key = KEYS[1]
    local dest_key = KEYS[2]
    local data = redis.call('DUMP', source_key)
    return redis.call('RESTORE', dest_key, data)
    """

    def __init__(self, *namespace, session=None, type_in_namespace=True,
                 pass_through=False):
        self.__session = None
        self.__type_in_namespace = type_in_namespace # Include the type class name in the namespace
        if session:
            self.bind(session)
        if not len(namespace) > 0:
            raise ValueError('`RedisObj` requires at least one namespace '
                             'argument.')
        self._namespace = namespace
        if pass_through is True:
            # Apply PassThrough functions to this instance.
            for attr,val in PassThrough.__dict__.items():
                if not attr.startswith('__'):
                    if isinstance(val, staticmethod):
                        val = val.__func__
                    setattr(self, attr, val)

        self.lua_copy = self.session.register_script(self.l_copy)
        log.debug("RedisObj instantiated. namespace: %s" %
                                                (':'.join(self._namespace)))

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
        if self.__type_in_namespace is True:
            return self._namespace + (self.__class__.__name__.lower(), )
        else:
            return self._namespace

    def bind(self, session):
        if isinstance(session, thredis.UnifiedSession):
            self.__session = session
            self.__session.bind_exec_event(self._execute)
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
        return self.keyspace_separator.join(self.namespace +
                                                tuple(suffix))

    def copy(self, target_model):
        source_key = self.gen_key()
        dest_key = target_model.gen_key()
        return self.lua_copy(keys=[source_key, dest_key])

    def __del__(self):
        pass

    # General model actions
    def delete(self, *keyspace):
        if len(keyspace) < 1:
            raise ValueError("Requires at least one positional argument for keyspace.")
        key = self.gen_key(*keyspace)
        return self.session.delete(key)

    def keys(self, *keyspace):
        key = self.gen_key(*keyspace)
        return self.session.keys(key)

    # Utilities
    @staticmethod
    def _ingress(val):
        """Ingress data."""
        if isinstance(val, bytes):
            return thredis.util.json.loads(val.decode())

        elif isinstance(val, list):
            return [thredis.util.json.loads(v.decode())
                        for v in val if v is not None]

        elif isinstance(val, tuple):
            return tuple([thredis.util.json.loads(v.decode())
                            for v in val if v is not None])

        elif isinstance(val, set):
            return set([thredis.util.json.loads(v.decode())
                            for v in val if v is not None])

        elif isinstance(val, dict):
            return thredis.util.json.loadd(val)

        elif val:
            return thredis.util.json.loads(val)

    @staticmethod
    def _egress(*args):
        # This should be more opposite of what _ingress is, but lets
        #   check implementation again.

        # Used as *arglist
        return tuple([thredis.util.json.dumps(val) for val in args])


class _String(RedisObj):
    """String Objects Adapter
    """
    # TODO: Add relevant LUAs. util.lua, etc.
    def get(self, keyspace=()):
        key = self.gen_key(*keyspace)
        return key, self.session.get(key)

    def set(self, val, keyspace=()):
        key = self.gen_key(*keyspace)
        return key, self.session.set(key, val)

    def delete(self, keyspace=()):
        key = self.gen_key(*keyspace)
        return key, self.session.delete(key)


class String(_String):
    def get(self, **kwa):
        key, raw = _String.get(self, **kwa)
        return key, self._ingress(raw)

    def set(self, value, **kwa):
        key, raw = _String.set(self, *self._egress(value), **kwa)
        return key, raw

    def delete(self, **kwa):
        key, raw = _String.delete(self, **kwa)
        return key, raw


class _List(RedisObj):
    """List Object Adapter
    """
    def count(self, keyspace=()):
        key = self.gen_key(*keyspace)
        return key, self.session.llen(key)

    def range(self, from_idx, to_idx, keyspace=()):
        key = self.gen_key(*keyspace)
        return key, self.session.lrange(key, from_idx, to_idx)

    def lpush(self, *objs, keyspace=()):
        key = self.gen_key(*keyspace)
        return key, self.session.lpush(key, *objs)

    def rpush(self, *objs, keyspace=()):
        key = self.gen_key(*keyspace)
        return key, self.session.rpush(key, *objs)

    def set(self, idx, obj, keyspace=()):
        key = self.gen_key(*keyspace)
        return key, self.session.lset(key, idx, obj)

    def lpop(self, keyspace=()):
        key = self.gen_key(*keyspace)
        return key, self.session.lpop(key)

    def blpop(self, keyspace=()):
        key = self.gen_key(*keyspace)
        return key, self.session.blpop(key)

    def rpop(self, keyspace=()):
        key = self.gen_key(*keyspace)
        return key, self.session.rpop(key)

    # Not API implemented yet.
    def linsert(self, pivot, value, before=None, after=None, keyspace=()):
        key = self.gen_key(*keyspace)
        return key, self.session.linsert(key, pivot, value, before=before,
                                         after=after)

class List(_List):
    """
    """
    def count(self, **kwa):
        """Return list count/length."""
        key, raw = self._count(**kwa)
        return key, raw

    def range(self, from_idx, to_idx, **kwa):
        key, raw = _List.range(self, from_idx, to_idx, **kwa)
        return key, self.ingress(raw)

    def all(self, **kwa):
        """Get all in list."""
        key, raw = _List.range(self, 0, -1, **kwa)
        return key, self._ingress(raw)

    def lpush(self, *objs, **kwa):
        """Insert obj(s) at start of list."""
        key, raw = _List.lpush(self, *self._egress(*objs), **kwa)
        return key, raw

    def rpush(self, *objs, **kwa):
        """Insert obj(s) at end of list."""
        key, raw = _List.rpush(self, *self._egress(*objs), **kwa)
        return key, raw

    def set(self, idx, obj, **kwa):
        """Set list item at `idx` to `obj`."""
        key, raw = _List.set(self, idx, *self._egress(obj), **kwa)
        return key, raw

    def get(self, idx, **kwa):
        """Get list item at `idx`."""
        key, raw = _List.range(self, idx, idx, **kwa)
        return key, self._ingress(raw)

    def lpop(self, **kwa):
        """Get first list item and remove it."""
        key, raw = _List.lpop(self, **kwa)
        return key, self._ingress(raw)

    def rpop(self, **kwa):
        """Get last list item and remove it."""
        key, raw = _List._rpop(self, **kwa)
        return key, self._ingress(raw)


class _Set(RedisObj):
    """
    """
    def count(self, keyspace=()):
        key = self.gen_key(*keyspace)
        return key, self.session.scard(key)

    def all(self, keyspace=()):
        key = self.gen_key(*keyspace)
        return key, self.session.smembers(key)

    def add(self, *objs, keyspace=()):
        key = self.gen_key(*keyspace)
        return key, self.session.sadd(key, *objs)

    def ismember(self, obj, keyspace=()):
        key = self.gen_key(*keyspace)
        return key, self.session.sismember(key, obj)

    def delete(self, *objs, keyspace=()):
        key = self.gen_key(*keyspace)
        return key, self.session.srem(key, *objs)

class Set(_Set):
    #Api functions.
    def count(self, **kwa):
        key, raw = _Set.count(self, **kwa)
        return key, raw

    def all(self, **kwa):
        key, raw = _Set.all(self, **kwa)
        return key, self._ingress(raw)

    def add(self, *objs, **kwa):
        key, raw = _Set.add(self, *self._egress(*objs), **kwa)
        return key, raw

    def ismember(self, obj, **kwa):
        key, raw = _Set.ismember(self, *self._egress(obj), **kwa)
        return key, raw

    def delete(self, *objs, **kwa):
        return _Set.delete(self, *self._egress(*objs), **kwa)


class _Hash(RedisObj):
    """
    """
    def get(self, id_, *fields, keyspace=()):
        key = self.gen_key(*keyspace+(id_,))
        if fields:
            return key, self.session.hmget(key, *fields)
        else:
            return key, self.session.hgetall(key)

    def set(self, id_, obj, keyspace=()):
        key = self.gen_key(*keyspace+(id_,))
        return key, self.session.hmset(key, obj)

    def delete(self, id_, keyspace=()):
        key = self.gen_key(*keyspace+(id_,))
        return key, self.session.delete(key)


class Hash(_Hash):
    """
    """
    @staticmethod
    def _egress(obj):
        return thredis.util.json.dumpd(obj)

    def get(self, id_, *fields, **kwa):
        key, raw =_Hash.get(self, id_, *fields, **kwa)
        return key, self._ingress(raw)

    def set(self, id_, obj, **kwa):
        key, raw = _Hash.set(self, id_, self._egress(obj), **kwa)
        return key, raw

    def delete(self, id_, **kwa):
        key, raw = _Hash._delete(self, id_, **kwa)
        return key, raw


class _ZSet(RedisObj):
    """
    """
    def __init__(self, *namespace, session=None, type_in_namespace=True,
                 pass_through=False, asc=False):
        RedisObj.__init__(self, *namespace, session=session,
                          pass_through=pass_through,
                          type_in_namespace=type_in_namespace)
        self._asc=asc

    def range(self, from_idx, to_idx, reversed=False, withscores=False):
        key = self.gen_key()
        direction = reversed and not self._asc or self._asc
        zrange = (direction and self.session.zrange or self.session.zrevrange)
        return key, zrange(key, int(from_idx), int(to_idx),
                            withscores=withscores)

    def rangebyscore(self, min_, max_, reversed=False, withscores=False):
        key = self.gen_key()
        direction = reversed and not self._asc or self._asc
        zrangebyscore = (direction and self.session.zrangebyscore or
                         self.session.zrevrangebyscore)
        return key, zrangebyscore(key, max_, min_,
                                  withscores=withscores)

    def all(self, reversed=False, withscores=False):
        return self.range(0, -1, reversed=reversed, withscores=withscores)

    def get(self, idx, reversed=False, withscores=False):
        return self.range(idx, idx, reversed=reversed, withscores=withscores)

    def add(self, obj, score=None):
        key = self.gen_key()
        return key, self.session.zadd(key, score, obj)

    def delete(self, obj):
        key = self.gen_key()
        return key, self.session.zrem(key, obj)

    def score(self, obj):
        key = self.gen_key()
        return key, self.session.zscore(key, obj)

    def count(self):
        key = self.gen_key()
        return key, self.session.zcard(key)


class ZSet(_ZSet):
    """
    """
    def count(self):
        return key, _ZSet.count(self)

    def range(self, from_idx, to_idx, reversed=False, withscores=False):
        key, lst = _ZSet.range(self, from_idx, to_idx, reversed=reversed,
                               withscores=withscores)
        if withscores is True:
            vals, scores = zip(*lst)
            return tuple(zip(self._ingress(vals), scores))
        else:
            return self._ingress(lst)

    def rangebyscore(self, min_, max_, reversed=False, withscores=False):
        key, lst = _ZSet.rangebyscore(self, min_, max_, reversed=reversed,
                                      withscores=withscores)
        if withscores is True:
            vals, scores = zip(*lst)
            return tuple(zip(self._ingress(vals), scores))
        else:
            return self._ingress(lst)

    def all(self, reversed=False, withscores=False):
        key, lst = _ZSet.all(self, reversed=reversed, withscores=withscores)

        if withscores is True:
            vals, scores = zip(*lst)
            return tuple(zip(self._ingress(vals), scores))
        else:
            return self._ingress(lst)

    def get(self, idx, reversed=False, withscores=False):
        key, vals = _ZSet.get(self, idx,
                                       reversed=reversed,
                                       withscores=withscores)
        if vals:
            val = vals[0]

            if withscores is True:
                val,score = val
                return (self._ingress(val), score)
            else:
                return self._ingress(val)


    def getbyscore(self, score, reversed=False, withscores=False):
        return self.rangebyscore(score, score, reversed=reversed,
                                 withscores=withscores)

    def score(self, obj):
        return _ZSet.score(self, *self._egress(obj))

    def add(self, obj, score=None):
        return _ZSet.add(self, *self._egress(obj), score=score)

    def delete(self, obj):
        return _ZSet.delete(self, *self._egress(obj))


class Nonces(_List):
    """
    * No ingress or egress json. This is meant to be super fast.
    """
    def __init__(self, *namespace, session=None, type_in_namespace=False):
        List.__init__(self, *namespace, session=session,
                      type_in_namespace=type_in_namespace)

    def gen(self, n):
        for _ in range(n):
            _List.rpush(self, thredis.util.nonce())

    def count(self):
        key, val = _List.count(self)
        return val

    def get(self):
        key, val = _List.blpop(self)
        return val


# Advanced types.
class Lock(RedisObj):
    """Simple Lock Primitive based on http://redis.io/commands/set. We also
    use LUA for speed here in dealing with the nonces. Nonces are usually added by a daemon to the
    `nonces_key`."""

    #race condition exists where the process takes longer than the deadlock
    #   timeout. This must NEVER happen.

    def __init__(self, *namespace, session=None, type_in_namespace=False,
                 timeout_ms=5000): 
        RedisObj.__init__(self, *namespace, session=session,
                          type_in_namespace=type_in_namespace)
        self._timeout = timeout_ms # deadlock timeout

        # This will change with new lua registry.
        self._lua_acquire = self.session.register_script(self.l_acquire_lock)
        self._lua_release = self.session.register_script(self.l_release_lock)

    def acquire(self, id_):
        return  self.lua_acquire(self.gen_key(id_), self._timeout)

    def release(self, id_, nonce):
        return self.lua_release(self.gen_key(id_), nonce)




# Start actual models.
class ModelObject:
    modelspace = 'model'
    schema = {}
    child_models = {}
    unique = set()

    # Used to provide a return from the models.
    feedback = SafeDict()

    def __init__(self, *namespace, session=None):
        assert session is not None
        self.__namespace = namespace
        self.s = session
        if self.schema:
            self.models = dict(self.__build_models())
            self.models_keys = set(self.models.keys())
        if self.child_models:
            self.children = dict(self.__build_submodels())
            self.children_keys = set(self.children.keys())

    @property
    def namespace(self):
        return ':'.join((self.modelspace,) + self.__namespace)

    def __build_models(self):
        for name, ModelCls in self.schema.items():
            yield name, ModelCls(self.namespace,
                                    ModelCls.__name__.lower(),
                                    name, session=self.s)

    def __build_submodels(self):
        for key, SubModelCls in self.child_models.items():
            yield key, SubModelCls(self.namespace, supermodel=self)


class SubModelObject(ModelObject):
    def __init__(self, *namespace, supermodel):
        if isinstance(supermodel, ModelObject):
            self.supermodel = supermodel
        else:
            raise Exception("SubModelObject requires a ModelObject `supermodel`.")
        ModelObject.__init__(self, *namespace, session=supermodel.s)


class Record(ModelObject):
    """
    """
    @staticmethod
    def _ingress(obj):
        """Ingress from client."""
        obj['_id'] = uuid.UUID(obj['_id'])
        return obj

    @staticmethod
    def _egress(obj):
        """Egress to client."""
        obj['_id'] = str(obj['_id'])
        return obj

    def _retrieve(self, location_id):
        record = self.models['record']
        key, val = record.get(location_id)
        log.debug("Record._retrieve: key: %s, val: %s" % (key, val))
        obj = self._egress(val)

        # check child records.
        for child_key in self.children_keys:
            sub = self.children[child_key]
            obj[child_key] = sub.retrieve(obj['_id'])

        return obj

    retrieve = _retrieve

    def _update(self, **obj):
        print("Record._update: obj: %s" % obj)
        active_model = self.models['active']
        every_model = self.models['all']
        record_model = self.models['record']

        obj = self._ingress(obj)

        obj_items = set(obj.items())
        obj_keys = set(obj.keys())

        _, all_ = every_model.all()

        # Check unique "constraint".
        for id_ in all_ - {obj['_id']}: # All but this object.
            _, val = record_model.get(id_, *self.unique)
            kv_uniques = zip(self.unique, val)
            for key, val in set(kv_uniques) & obj_items:
                raise UniqueFailed("Unique constraint failed on id: %s  {%s: %s}" % (id_, key, val))

        # check child records.
        # TODO: Move this elsewhere.
        #       Possibly removing the self.children and self.child_models
        #           attributes.
        for child_key in obj_keys & self.children_keys:
            sub = self.children[child_key]
            val = obj[child_key]
            # if val is not None: # I hate this here. HAH!
            sub.update(obj['_id'], *obj[child_key])
            del obj[child_key]

        # Do record updates
        if '_active' in obj:
            if obj['_active'] is True:
                active_model.add(obj['_id'])
            else:
                active_model.delete(obj['_id'])

        if '_id' in obj:
            every_model.add(obj['_id'])
            record_model.set(obj['_id'], obj)
        else:
            raise Exception("model update requires _id value")
    update = _update

    def all(self, **obj):
        # Let's get in to the "location:active:set"
        logging.debug('Model all! obj: %s' % obj)
        record = self.models['record']

        if obj.get('active_only') is True:
            active = self.models['active']
            _, vals = active.all()
            return [self.retrieve(id_) for id_ in vals]
        else:
            every = self.models['all']
            _, vals = every.all()
            return [self.retrieve(id_) for id_ in vals]

    def create(self, **obj):
        obj['_id'] = uuid.uuid4()
        obj['_active'] = True
        obj = self._egress(obj) # Egress as if this data came from a client.
        self.update(**obj)
        return obj

    def delete(self, **obj):
        obj['_active'] = False
        self.update(**obj)


