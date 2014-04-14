""" """


# ** Warning: these data libraries are not compatible with nested
#   dictionaries. Since Redis essentially offers that with "Hashes",
#   nested dictionary compatibility is low priority.
#
#   If you feel you need to nest dictionaries, consider utilizing redis
#   hashes further. **

import logging
log = logging.getLogger(__name__)

import collections
import uuid
import json as _json


__all__ = ('HashableOrderedDict', 'JsonEncoder', 'JsonDecoder', 'json')


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


class JsonEncoder(_json.JSONEncoder):
    """Simple encoder to handle UUID types. Converts to URN (Universal
    Resource Name); "urn:uuid:1111-..."
    
    """
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return obj.urn
        if isinstance(obj, bytes):
            obj = obj.decode()
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