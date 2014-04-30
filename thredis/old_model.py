# Borked. Don't use
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
        return self._ingress(self.r_range(from_idx, to_idx, reversed))

    def all(self, reversed=False):
        return self._ingress(self.r_range(0,-1, reversed))

    def get(self, idx):
        return self._ingress(self.r_get(idx))

    def add(self, obj):
        return self.r_add(*self._egress(obj))

    def between(self, low_idx, high_idx, obj):
        return self.r_between(low_idx, high_idx, *self._egress(obj))

    def insert(self, idx, obj):
        return self.r_between(idx, idx-1, *self._egress(obj))

    def delete(self, obj):
        return self.r_delete(*self._egress(obj))
        

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

    def delete(self, _id):
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

    def delete(self):
        # TODO: Rename any other "delete" methods that aren't directly
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

    def delete(self):
        """Removes this entire collection from Redis."""
        self.__active_index.delete()
        for _id in self.__index:
            self.__hash.delete(_id)
        self.__index.delete()

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


'''
class Hash(RedisObj):
    """
    """

    @staticmethod
    def _egress(**obj):
        return json.dumpd(obj)

    # Low functions
    def r_get(self, key):
        key = self.gen_key(key)
        return self.session.hgetall(key)

    def r_set(self, key, obj):
        key = self.gen_key(key)
        return self.session.hmset(key, obj)

    # API functions
    def get(self, key):
        return self._ingress(self.r_get(key))

    def set(self, key, obj):
        return self.r_set(key, self._egress(**obj))

    def delete(self, key):
        key = self.gen_key(key)
        return self.session.delete(key)

'''


## Extended Models
# This is where the concept of internal fields comes in to play.


'''
class Record(Hash):
    """
    """
    ingressed_demarc = '_'
    egressed_demarc = '__'
    internal_keys = {'id', 'active', 'created', 'updated'}
    internal_defaults = {'id': lambda rec: rec._key_func(),
                         'active': True,
                         'created': lambda rec: rec._time_func(),
                         'updated': lambda rec: rec._time_func()}

    def __init__(self, *namespace, key_func=uuid.uuid4, time_func=time.time,
                    **kwa):
        Hash.__init__(self, *namespace, **kwa)
        self._key_func = key_func
        self._time_func = time_func

    @property
    def egressed_internal_keys(self, *args):
        # Return a set of internal keys as egressed (persisted)
        return {self.egressed_demarc+key for key in self.internal_keys |
                    set(args)}

    @property
    def ingressed_internal_keys(self, *args):
        # Return a set of internal keys as ingressed (loaded)
        return {self.ingressed_demarc+key for key in self.internal_keys |
                    set(args)}


    def _ingress_keys(self, *keys):
        # From egressed to ingressed.

        egressed_keys = set(keys) & self.egressed_internal_keys

        return set(egressed.replace(self.egressed_demarc,
                                    self.ingressed_demarc)
                    for egressed in egressed_keys)


    def _egress_keys(self, *keys):
        # From ingressed to egressed.

        ingressed_keys = set(keys) & self.ingressed_internal_keys

        return set(ingressed.replace(self.ingressed_demarc,
                                     self.egressed_demarc)
                    for ingressed in ingressed_keys)



    def _ingress(self, obj):
        """Standard object ingress as well as some internal_key handling."""
        obj = RedisObj._ingress(obj)

        obj_key_names = set(obj.keys())

        pulled_keys = self.internal_keys & obj_key_names

        for pulled in pulled_keys:
            obj[self.internal_key_demarc+pulled] = obj[pulled]
            del obj[pulled]

        return obj

    def _egress(self, obj):
        # Expects internals to be prepended with '_'
        obj_key_names = set(obj.keys())
        pushed_key_names = set(self.internal_key_demarc+key for
                                    key in self.internal_keys)

        for key in obj_key_names & pushed_key_names:
            obj[key.lstrip('_')] = obj[key]
            del obj[key]

        return Hash._egress(**obj)

    def create(self, obj):
        obj_key_names = set(obj.keys())
        pushed_key_names = set(self.internal_key_demarc+key for
                                    key in self.internal_keys)

        # Set defaults if do not exist.
        # TODO: check out defaultdict for this.
        missing_keys = pushed_key_names - obj_key_names
        for missing in missing_keys:
            default = self.internal_defaults.get(missing.lstrip('_'))
            if callable(default):
                obj[missing] = default(self)
            else:
                obj[missing] = default

    def get(self, id_):
        obj = Hash.get(self, id_)

    def set(self, obj, **opts):
        key = self.gen_key(*keyspace)

    def delete(self, id_, reference=True):
        """
        """
        if reference is True:
            pass
        else:
            Hash.delete(self, id_)'''
################################################################################

