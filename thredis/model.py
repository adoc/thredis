""" """
import logging
log = logging.getLogger(__name__)

import uuid
import thredis.util


__all__ = ('ConstraintFailed', 'UniqueFailed', 'ModelObject', 'Record')


class ConstraintFailed(Exception):
    pass


class UniqueFailed(ConstraintFailed):
    pass


# These aren't really models. More like part of the session!!!!
# TODO: Work out how these will fit in to the session it self.


# Start actual models.
class ModelObject:
    modelspace = 'model'
    schema = {}
    child_models = {}
    unique = set()

    # Used to provide a return from the models.
    feedback = thredis.util.safedict()

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
        print("CREATE")
        obj['_id'] = uuid.uuid4()
        obj['_active'] = True
        obj = self._egress(obj) # Egress as if this data came from a client.
        self.update(**obj)
        return obj

    def delete(self, **obj):
        obj['_active'] = False
        self.update(**obj)


