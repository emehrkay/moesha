import copy

from .property import (PropertyManager, Property)
from .util import (normalize_labels, entity_name, entity_to_labels)



ENTITY_MAP = {}


class _Entity(type):

    def __new__(cls, name, bases, attrs):
        """Inherit the Property objects, _ALLOW_UNDEFINED, and _LABELS
        attributes from the bases"""
        glob = {
            'allow_undefined': None,
            'labels': None,
        }
        properties = {}


        def get_props(source):
            if '_ALLOW_UNDEFINED' in source:
                glob['allow_undefined'] = bool(source.get('_ALLOW_UNDEFINED'))

            if '_LABELS' in source:
                glob['labels'] = source.get('_LABELS', None)

            for n, v in source.items():
                if isinstance(v, Property):
                    properties[n] = v


        def walk(bases):
            walk_bases = list(bases)

            walk_bases.reverse()

            for wb in walk_bases:
                walk(wb.__bases__)

            [get_props(b.__dict__) for b in walk_bases]


        walk(bases)
        get_props(attrs)


        def build_properties(self, data_type):
            props = {n: copy.deepcopy(p) for n, p in properties.items()}
            self.properties = PropertyManager(properties=props,
                allow_undefined=glob['allow_undefined'], data_type=data_type)


        if glob['labels']:
            if not isinstance(glob['labels'], (list, tuple, set)):
                glob['labels'] = [glob['labels'],]

            labels = normalize_labels(*glob['labels'])
        else:
            labels = entity_to_labels(cls)

        ENTITY_MAP[labels] = entity_name(cls)
        cls = super(_Entity, cls).__new__(cls, name, bases, attrs)
        setattr(cls, '_build_properties', build_properties)

        return cls


class Entity(metaclass=_Entity):
    _LABELS = None

    def __init__(self, data_type='python', labels=None, id=None,
                 properties=None):
        self._build_properties(data_type=data_type)
        self._data_type = 'python'
        self.data_type = data_type
        self.label = labels or self._LABELS
        self._id = None
        self.id = id
        properties = properties or {}

        self.force_hydrate(**properties)

    def safely_stringify_for_pudb(self):
        return None

    def _get_data_type(self):
        return self.properties.data_type

    def _set_data_type(self, data_type):
        self.properties.data_type = data_type

        return self

    data_type = property(_get_data_type, _set_data_type)

    def _get_labels(self):
        self._LABELS.sort()

        return self._LABELS

    def _set_labels(self, label):
        if not label:
            label = entity_to_labels(self).split(':')

        if not isinstance(label, (list, set, tuple)):
            label = [label,]

        label.sort()

        self._LABELS = label

        return self

    label = property(_get_labels, _set_labels)

    def _get_id(self):
        return self._id

    def _set_id(self, id):
        if not self._id:
            self._id = id

    id = property(_get_id, _set_id)

    def hydrate(self, **properties):
        self.properties.hydrate(**properties)

        return self

    def force_hydrate(self, **properties):
        self.properties.force_hydrate(**properties)

        return self

    def __getitem__(self, field):
        return self.properties[field]

    def __setitem__(self, field, value):
        self.properties[field] = value

    def __delitem__(self, field):
        del(self.properties[field])

    @property
    def data(self):
        return self.properties.data


class Node(Entity):
    _ALLOW_UNDEFINED = True


class StructuredNode(Node):
    _ALLOW_UNDEFINED = False


class Relationship(Entity):
    _ALLOW_UNDEFINED = True


class StructuredRelationship(Relationship):
    _ALLOW_UNDEFINED = False
