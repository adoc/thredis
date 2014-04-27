"""
Python Pseudocode implementation of a unique constraint.

uniques  - Set of unique KEYS. Ex: {'username'}
obj      - Dict object being persisted. Ex: {'id': 12345,
                                             'username'}
obj_set  - Set of `obj` key/val pairs.
all_     - Set of ID of every record.
id_      - ID of object we are checking

Hash     - Mimics Redis Hash objects and retreival (only).
"""


class UniqueFailed(Exception):
    pass


class Hash:
    """Mimics Redis Hash objects and retreival (only)."""
    objs = {12345:
                {'id': 12345,
                 'username': 'nick',
                 'password': '12345',
                 'description': 'mala'},
            54321:
                {'id': 54321,
                'username': 'ken',
                'password': '54321',
                'description': 'unsure!'}
            }

    allkeys = set(objs.keys())

    @staticmethod
    def _get(id_, *keys):
        for key in keys:
            yield key, Hash.objs[id_][key]

    @classmethod
    def get(cls, id_, *keys):
        """Get only keys specified from dict."""
        return tuple(cls._get(id_, *keys))


def _check_unique(obj, uniques):
    obj_set = set(obj.items())
    for id_ in Hash.allkeys - {obj['id']}: # Iterate all IDs but this objects.
        check = Hash.get(id_, *uniques)
        for key, val in set(check) & obj_set: # Intersect to find matches and iterate over pairs.
            raise UniqueFailed("Unique constraint failed on id: %s  {%s: %s}" % (id, key, val))


def check_unique(obj, uniques):
    try:
        _check_unique(obj, uniques)
    except UniqueFailed:
        return False
    else:
        return True


if __name__ == '__main__':
    uniques = {'username'} # No need to constrain ID as Hash/dict already does this for us.

    assert check_unique({'id': 1,
                         'username': 'coda',
                         'password': 12345,
                         'description': 'mala'}, uniques) is True

    assert check_unique({'id': 12345,
                         'username': 'nick',
                         'password': 12345,
                         'description': 'mala'}, uniques) is True
    
    assert check_unique({'id': 54321,
                         'username': 'ken',
                         'password': 12345,
                         'description': 'mala'}, uniques) is True

    assert check_unique({'id': 1,
                         'username': 'ken',
                         'password': 12345,
                         'description': 'mala'}, uniques) is False

    assert check_unique({'id': 12345,
                         'username': 'ken',
                         'password': 12345,
                         'description': 'mala'}, uniques) is False

    assert check_unique({'id': 54321,
                         'username': 'nick',
                         'password': 12345,
                         'description': 'mala'}, uniques) is False