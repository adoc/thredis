""" """
import logging
log = logging.getLogger(__name__)


import threading
import base64
import time
import uuid
from thredis import UnifiedSession
from thredis.util import json, nonce_h

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
        if isinstance(session, UnifiedSession):
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
            return json.loads(val.decode())

        elif isinstance(val, list):
            return [json.loads(v.decode()) for v in val if v is not None]

        elif isinstance(val, tuple):
            return tuple([json.loads(v.decode()) for v in val if v is not None])

        elif isinstance(val, set):
            return set([json.loads(v.decode()) for v in val if v is not None])

        elif isinstance(val, dict):
            return json.loadd(val)

        elif val:
            return json.loads(val)

    @staticmethod
    def _egress(*args):
        # This should be more opposite of what _ingress is, but lets
        #   check implementation again.

        # Used as *arglist
        return tuple([json.dumps(val) for val in args])




class String(RedisObj):
    """
    """
    def _get(self):
        key = self.gen_key()
        return key, self.session.get(key)

    def _set(self, val):
        key = self.gen_key()
        return key, self.session.set(key, val)

    # API functions
    def get(self):
        key, raw = self._get()
        return {'key': key,
                'obj': self._ingress(raw),
                'raw': raw}

    def set(self, value):
        key, raw = self._set(*self._egress(value))
        return {'key': key,
                'obj': value,
                'raw': raw}

    def delete(self):
        key = self.gen_key()
        return {'key': key,
                'raw': self.session.delete(key)}


class Lock(RedisObj):
    """Simple Lock Primitive based on http://redis.io/commands/set. We also
    use LUA for speed here in dealing with the nonces. Nonces are usually added by a daemon to the
    `nonces_key`."""

    l_acquire_lock = """
        -- Attempt to acquire a lock on resource_key
        local resource_key = KEYS[1]
        local nonces_key = KEYS[2]
        local nonce = redis.call("LPOP", nonces_key)
        local timeout_ms = tostring(ARGV[1])
        local timestamp = tostring(ARGV[2])
        local lock_key = "lock:" .. resource_key
        local lock_key_ts = "lock_ts:" .. resource_key
        -- Set lock to nonce(id) with deadlock timeout_ms.
        if nonce then
            if redis.call("SET", lock_key, nonce, "NX", "PX", timeout_ms) then
                -- We got the lock, set the timestamp and return the NONCE.
                redis.call("SET", lock_key_ts, timestamp, "PX", timeout_ms)
                return nonce
            else
                -- We got a nonce, but could not get the lock, return FALSE.
                return false
            end
        else
            -- No nonce available, return NIL.
            return nil
        end
    """

    # Not sure I need this.
    l_check_lock = """
        -- Verify this nonce has the lock. This should help with deadlock
        --   expiring locks but process still thinks it has the lock?
    """

    l_release_lock = """
        -- Attempt to release the lock.
        local resource_key = KEYS[1]
        local key = "lock:" .. resource_key
        local token = ARGV[1]
        if redis.call("GET", key) == token then
            return redis.call("DEL", key)
        else
            return nil
        end
    """
    
    # Set lock
    # SET resource-name anystring NX EX max-lock-time
    # Unset Lock (Called by whom?)
    # EVAL ...script... 1 resource-name token-value

    nonces_key = 'nonces'

    #race condition exists where the process takes longer than the deadlock
    #   timeout. This must NEVER happen.

    def __init__(self, *namespace, session=None, type_in_namespace=False,
                 timeout_ms=5000, gen_id=False, id=None): 
        RedisObj.__init__(self, *namespace, session=session,
                          type_in_namespace=type_in_namespace)
        self._timeout = timeout_ms # deadlock timeout
        self._lua_acquire = self.session.register_script(self.l_acquire_lock)
        self._lua_release = self.session.register_script(self.l_release_lock)
        self._gen_id = gen_id
        self._id = id

    @property
    def ts(self):
        model = String('lock_ts', *self._namespace, session=self.session,
                        type_in_namespace=False)
        key, val = model._get()
        return float(val)

    def acquire(self):
        if self._gen_id is True:
            # Testing only. Use Nonce generator and gen_id=False.
            nonce = nonce512()
            if self.session.client.set('lock:'+self.gen_key(), nonce, nx=True,
                        px=self._timeout):
                return self._id
        else:
            # The id of this lock is assigned by redis via the `nonces_key`
            nonce = self._id = self._lua_acquire(keys=[self.gen_key(), self.nonces_key],
                                 args=[self._timeout, time.time()])
            return nonce

        # Did not acquire the lock
        return False

    def release(self):
        return self._lua_release(keys=[self.gen_key()], args=[self._id])


class List(RedisObj):
    """
    """

    # Low functions
    def _count(self):
        key = self.gen_key()
        return key, self.session.llen(key)

    def _range(self, from_idx, to_idx):
        key = self.gen_key()
        return key, self.session.lrange(key, from_idx, to_idx)

    def _lpush(self, *objs):
        key = self.gen_key()
        return key, self.session.lpush(key, *objs)

    def _rpush(self, *objs):
        key = self.gen_key()
        return key, self.session.rpush(key, *objs)

    def _set(self, idx, obj):
        key = self.gen_key()
        return key, self.session.lset(key, idx, obj)

    def _lpop(self):
        key = self.gen_key()
        return key, self.session.lpop(key)

    def _blpop(self):
        key = self.gen_key()
        return key, self.session.blpop(key)

    def _rpop(self):
        key = self.gen_key()
        return key, self.session.rpop(key)

    def _linsert(self, pivot, value, before=None, after=None):
        key = self.gen_key()
        return key, self.session.linsert(key, pivot, value, before=before, after=after)


    # API functions
    def all(self):
        """Get all in list."""
        # return self._ingress(self.r_range(0, -1))
        key, raw = self._range(0, -1)
        return {'key': key,
                'obj': self._ingress(raw),
                'raw': raw}

    def count(self):
        """Return list count/length."""
        key, raw = self._count()
        return {'key': key,
                'raw': raw}

    def lpush(self, *objs):
        """Insert obj(s) at start of list."""
        #return self.r_lpush(*self._egress(*objs))
        key, raw = self._lpush(*self._egress(*objs))
        return {'key': key,
                'obj': objs,
                'raw': raw}

    def rpush(self, *objs):
        """Insert obj(s) at end of list."""
        #return self.r_rpush(*self._egress(*objs))
        key, raw = self._rpush(*self._egress(*objs))
        return {'key': key,
                'obj': objs,
                'raw': raw}

    def set(self, idx, obj):
        """Set list item at `idx` to `obj`."""
        # return self.r_set(idx, *self._egress(obj))
        key, raw = self._set(idx, *self._egress(obj))
        return {'key': key,
                'obj': obj,
                'raw': raw}

    def get(self, idx):
        """Get list item at `idx`."""
        #return self._ingress(self.r_range(idx, idx))
        key, raw = self._range(idx, idx)
        return {'key': key,
                'obj': self._ingress(raw),
                'raw': raw}

    def lpop(self):
        """Get first list item and remove it."""
        #return self._ingress(self.r_lpop())
        key, raw = self._lpop()
        return {'key': key,
                'obj': self._ingress(raw),
                'raw': raw}

    def rpop(self):
        """Get last list item and remove it."""
        #return self._ingress(self.r_rpop())
        key, raw = self._rpop()
        return {'key': key,
                'obj': self._ingress(raw),
                'raw': raw}

    '''
    # Don't need or use?
    def before(self, pivot, value):
        """ """
        #return self.r_linsert(self._egress(pivot),
        #                      self._egress(value), before=True)
        return {'key': key,
                'obj': value,
                'raw': self._linsert(self._egress(pivot),
                              self._egress(value), before=True)}

    def after(self, pivot, value):
        """ """
        #return self.r_linsert(self._egress(pivot),
        #                      self._egress(value), after=True)
        return {'key': key,
                'obj': value,
                'raw': self._linsert(self._egress(pivot),
                              self._egress(value), after=True)}
    '''


class Set(RedisObj):
    """
    """

    def _all(self):
        key = self.gen_key()
        return self.session.smembers(key)

    def _add(self, *objs):
        key = self.gen_key()
        return self.session.sadd(key, *objs)

    def _ismember(self, obj):
        key = self.gen_key()
        return self.session.sismember(key, obj)

    def _delete(self, *objs):
        key = self.gen_key()
        return self.session.srem(key, *objs)

    #Api functions.
    def count(self):
        key = self.gen_key()
        return self.session.scard(key)
        return {'key': key,
                }

    def all(self):
        return self._ingress(self.r_all())

    def add(self, *objs):
        return self.r_add(*self._egress(*objs))

    def ismember(self, obj):
        return self.r_ismember(*self._egress(obj))

    def delete(self, *objs):
        return self.r_delete(*self._egress(*objs))


class Hash(RedisObj):
    """
    """

    @staticmethod
    def _egress(obj):
        return json.dumpd(obj)

    # Low functions
    def r_get(self, key, *fields):
        key = self.gen_key(key)
        if fields:
            return self.session.hmget(key, *fields)
        else:
            return self.session.hgetall(key)

    def r_set(self, key, obj):
        key = self.gen_key(key)
        return self.session.hmset(key, obj)

    # API functions
    def get(self, key, *fields):
        return self._ingress(self.r_get(key, *fields))

    def set(self, key, obj):
        return self.r_set(key, self._egress(obj))

    def delete(self, key):
        key = self.gen_key(key)
        return self.session.delete(key)


class ZSet(RedisObj):
    """
    """
    def __init__(self, *namespace, session=None, type_in_namespace=True,
                 pass_through=False, asc=False):
        RedisObj.__init__(self, *namespace, session=session,
                          pass_through=pass_through,
                          type_in_namespace=type_in_namespace)
        self._asc=asc

    def _range(self, from_idx, to_idx, reversed=False, withscores=False):
        key = self.gen_key()
        direction = reversed and not self._asc or self._asc
        zrange = (direction and self.session.zrange or self.session.zrevrange)
        return key, zrange(key, int(from_idx), int(to_idx),
                            withscores=withscores)

    def _rangebyscore(self, min_, max_, reversed=False, withscores=False):
        key = self.gen_key()
        direction = reversed and not self._asc or self._asc
        zrangebyscore = (direction and self.session.zrangebyscore or
                         self.session.zrevrangebyscore)
        return key, zrangebyscore(key, max_, min_,
                                  withscores=withscores)

    def _all(self, reversed=False, withscores=False):
        return self._range(0, -1, reversed=reversed, withscores=withscores)

    def _get(self, idx, reversed=False, withscores=False):
        return self._range(idx, idx, reversed=reversed, withscores=withscores)

    def _add(self, obj, score=None):
        key = self.gen_key()
        return key, self.session.zadd(key, score, obj)

    def _delete(self, obj):
        key = self.gen_key()
        return key, self.session.zrem(key, obj)

    def _score(self, obj):
        key = self.gen_key()
        return key, self.session.zscore(key, obj)

    def _count(self):
        key = self.gen_key()
        return key, self.session.zcard(key)

    # API
    def count(self):
        return self._count()

    def range(self, from_idx, to_idx, reversed=False, withscores=False):
        key, lst = self._range(from_idx, to_idx, reversed=reversed,
                               withscores=withscores)
        if withscores is True:
            vals, scores = zip(*lst)
            return tuple(zip(self._ingress(vals), scores))
        else:
            return self._ingress(lst)

    def rangebyscore(self, min_, max_, reversed=False, withscores=False):
        key, lst = self._rangebyscore(min_, max_, reversed=reversed,
                                      withscores=withscores)
        if withscores is True:
            vals, scores = zip(*lst)
            return tuple(zip(self._ingress(vals), scores))
        else:
            return self._ingress(lst)

    def all(self, reversed=False, withscores=False):
        key, lst = self._all(reversed=reversed, withscores=withscores)

        if withscores is True:
            vals, scores = zip(*lst)
            return tuple(zip(self._ingress(vals), scores))
        else:
            return self._ingress(lst)

    def get(self, idx, reversed=False, withscores=False):
        key, vals = self._get(idx,
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
        return self._score(*self._egress(obj))

    def add(self, obj, score=None):
        return self._add(*self._egress(obj), score=score)

    def delete(self, obj):
        return self._delete(*self._egress(obj))


# Start actual models.
class ModelObject:
    modelspace = 'model'
    schema = {}
    child_models = {}
    unique = set()

    # Used to provide a return from the models.
    feedback = SafeDict(threading.get_ident)

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
        obj = self._egress(record.get(location_id))

        # check child records.
        for child_key in self.children_keys:
            sub = self.children[child_key]
            obj[child_key] = sub.retrieve(obj['_id'])

        return obj

    retrieve = _retrieve

    def _update(self, **obj):
        active_model = self.models['active']
        every_model = self.models['all']
        record_model = self.models['record']

        obj = self._ingress(obj)

        obj_items = set(obj.items())
        obj_keys = set(obj.keys())

        all_ = every_model.all()

        # Check unique "constraint".
        for id_ in all_ - {obj['_id']}: # All but this object.
            kv_uniques = zip(self.unique, record_model.get(id_, *self.unique))

            for key, val in set(kv_uniques) & obj_items:
                raise UniqueFailed("Unique constraint failed on id: %s  {%s: %s}" % (id_, key, val))

        # check child records.
        # TODO: Move this elsewhere.
        #       Possibly removing the self.children and self.child_models
        #           attributes.
        for child_key in obj_keys & self.children_keys:
            sub = self.children[child_key]
            val = obj[child_key]
            if val: # I hate this here.
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

        if obj.get('active') is True:
            active = self.models['active']
            return [self.retrieve(id_) for id_ in active.all()]
        else:
            every = self.models['all']
            return [self.retrieve(id_) for id_ in every.all()]

    def create(self, **obj):
        obj['_id'] = uuid.uuid4()
        obj['_active'] = True
        obj = self._egress(obj) # Egress as if this data came from a client.
        self.update(**obj)
        return obj

    def delete(self, **obj):
        obj['_active'] = False
        self.update(**obj)


class Nonces(List):
    def __init__(self, *namespace, session=None, type_in_namespace=False):
        List.__init__(self, *namespace, session=session,
            type_in_namespace=type_in_namespace)

    def gen(self, n):
        for _ in range(n):
            #rnd = base64.b64encode(nonce(self.size)).decode()
            #rnd = rnd[:self._size]
            #assert len(rnd) == self._size
            self._rpush(nonce_h())

    def count(self):
        key, val = self._count()
        return val

    def get(self):
        key, val = self._blpop()
        return val