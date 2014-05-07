""" """


# ** Warning: these data libraries are not compatible with nested
#   dictionaries. Since Redis essentially offers that with "Hashes",
#   nested dictionary compatibility is low priority.
#
#   If you feel you need to nest dictionaries, consider utilizing redis
#   hashes further. **

import logging
log = logging.getLogger(__name__)

import os
import time
import base64
import threading
import hashlib
import collections
import datetime
import uuid
import json as _json

from safedict import SafeDict as safedict

try:
    import pytz
except ImportError:
    log.warn("Pytz not installed. Some functionality in will be affected.")
    pytz = None

binencode = base64.b64encode


threadfunc = lambda f: threading.Thread(target=f).start()

__all__ = ('safedict',  'threadfunc', 'nonce', 'HashableOrderedDict',
            'JsonEncoder', 'JsonDecoder', 'json', 'ModelLoopThread')



# 20 byte nonce. period.
nonce = lambda: hashlib.sha1(os.urandom(512 * 20)).digest()



'''
# This is all silly.
NONCE_ATOM = 16 
nonce = lambda: open('/dev/random', 'rb').read(NONCE_ATOM)
nonce_n = lambda n: open('/dev/random', 'rb').read(NONCE_ATOM * n)
nonce_h = lambda: hashlib.sha256(os.urandom(NONCE_ATOM ** 2)).digest()
nonce_nh = lambda n: hashlib.sha256(os.urandom(NONCE_ATOM ** n)).digest()'''


# Unused but saved for reference.
def decorate(decorator):
    """Will decorate a class or a single function.
    """
    
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


class HashableOrderedDict(collections.OrderedDict):
    """
    *** This hash does not support nested dicts.
    """
    def __hash__(self):
        return hash(frozenset(self))

# Roughly based on ISO8601
isoformat = "urn:timestamp:%Y-%m-%dT%H:%M:%S"


class JsonEncoder(_json.JSONEncoder):
    """Simple encoder to handle UUID types. Converts to URN (Universal
    Resource Name); "urn:uuid:1111-..."
    
    """
    def default(self, obj):
        if (isinstance(obj, datetime.date) or
            isinstance(obj, datetime.datetime)):
            zone = ''
            if obj.tzinfo:
                zone = 'PYTZ:'+obj.tzinfo.zone
            return obj.strftime(isoformat)+zone
        if isinstance(obj, uuid.UUID):
            return obj.urn
        if isinstance(obj, bytes):
            try:
                obj = obj.decode()
            except UnicodeDecodeError:
                obj = binencode(obj).decode()
                return _json.dumps(obj)
        return _json.JSONEncoder.default(self, obj)


class JsonDecoder(_json.JSONDecoder):
    """Simple decoder to handle UUID types. Instances UUID object when
    value starts with "urn:uuid".

    """
    def decode(self, obj):
        obj = _json.JSONDecoder.decode(self, obj)
        # Handle potential UUID.
        if isinstance(obj, str) and obj.startswith('urn:uuid'):
            return uuid.UUID(obj)
        if isinstance(obj, str) and obj.startswith('urn:timestamp'):
            ts, *zone = obj.split('PYTZ:')
            ts = datetime.datetime.strptime(ts, isoformat)
            if zone:
                if pytz is None:
                    raise Exception("pytz module required to parse this timestamp: %s" % obj)
                ts = pytz.timezone(*zone).localize(ts)
            return ts
        else:
            return obj


def qbytes(*args):
    """Accepts argument list of variables that might be bytes and
    decodes, otherwise passing the value through."""
    # This is a bit hackish and the static-typers are throwing their
    # hands up. :P
    return tuple([arg.decode()
                    if isinstance(arg, bytes)
                    else arg
                    for arg in args])


class json:
    """Monkeypatched Json functions."""

    @staticmethod
    def dumps(*args, **kwa):
        kwa['separators'] = kwa.get('separators', (',', ':'))
        kwa['sort_keys'] = True
        kwa['cls'] = kwa.get('cls', JsonEncoder)
        return _json.dumps(*args, **kwa)

    @staticmethod
    def loads(*args, **kwa):
        kwa['object_pairs_hook'] = kwa.get('object_pairs_hook',
                                            HashableOrderedDict)
        kwa['cls'] = kwa.get('cls', JsonDecoder)
        return _json.loads(*args, **kwa)

    @staticmethod
    def dumpd(obj, *args,  **kwa):
        """Dump dictionary values. This is a preparation for
        insertion in to the string-typed Redis Hash fields.
        """
        return {k: json.dumps(v, *args, **kwa) for k, v in obj.items() if v}

    @staticmethod
    def loadd(src, *args, **kwa):
        """Load dictionary values. This is after retrieval from
        Redis Hash.
        """
        obj = {}

        for k, v in src.items():
            k, v = qbytes(k, v)
            obj[k] = json.loads(v)
        return obj


class ModelLoopThread(threading.Thread):
    """Thread that loops indefinitely calling `callback` ever `throttle_ms`
    milliseconds. Also provides a redis `model` to the callback function.
    """
    _throttle = None
    def __init__(self, model=None, session=None, throttle_ms=100.0):
        threading.Thread.__init__(self)
        self.model = model
        self.session = session
        self._throttle = self._throttle or float(throttle_ms/1000.0)
        self._stack = 0

    def _callback(self):
        raise NotImplementedError("Subclass ModelLoop to implement!")

    def _nini(self):
        time.sleep(self._throttle)

    def run(self):
        while True:
            if self._stack < 1:
                self._nini()
                self._stack += 1
                self._callback()
                self._stack -= 1
                if self.session:
                    self.session.execute()
                elif self.model:
                    self.model.session.execute()
            else:
                print("skip")


def timedec(func):
    """Decorator to time a function and print out the results."""
    def innerfunc(*args, **kwa):
        t0=time.time()
        try:
            return func(*args,  **kwa)
        finally:
            name = func.__name__
            print("func `%s` timing: %s" % (name, time.time()-t0))
    return innerfunc