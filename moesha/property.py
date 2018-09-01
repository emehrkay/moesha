import copy
import json

from collections import OrderedDict
from datetime import datetime


class PropertyManager(object):

    def __init__(self, properties=None, data_type='python',
                 allow_undefined=False):
        self._properties = {}
        self.properties = properties or {}
        self._data_type = data_type
        self.data_type = data_type
        self.allow_undefined = allow_undefined

    def safely_stringify_for_pudb(self):
        return None

    def _set_data_type(self, data_type):
        self._data_type = data_type

        for name, field in self.properties.items():
            field.data_type = data_type

    def _get_data_type(self):
        return self._data_type

    data_type = property(_get_data_type, _set_data_type)

    def _get_properties(self):
        return self._properties

    def _set_properties(self, properties):
        for name, field in properties.items():
            self._properties[name] = field

    properties = property(_get_properties, _set_properties)

    @property
    def data(self):
        data = {n: f.value for n, f in self.properties.items()}

        return OrderedDict(sorted(data.items()))

    def __getitem__(self, field):
        if not field in self.properties:
            return None

        return self.properties[field].value

    def __setitem__(self, field, value):
        prop = self.get_property(field, value)

        if prop:
            prop.value = value

    def __delitem__(self, field):
        if field in self.properties:
            del(self.properties[field])

    def get_property(self, field, value=None):
        if field in self.properties:
            return self.properties[field]

        if self.allow_undefined:
            if isinstance(value, bool):
                obj = Boolean
            elif isinstance(value, str):
                obj = String
            elif isinstance(value, int):
                obj = Integer
            elif isinstance(value, float):
                obj = Float
            elif isinstance(value, datetime):
                obj = DateTime

            prop = obj(value=value, data_type=self.data_type, name=field)

            if prop not in self.properties:
                self.properties[field] = prop

        return None

    def hydrate(self, **properties):
        """Call model.hydrate to ensure that there will be no changes
        registered on the model's properties"""
        for field, value in properties.items():
            self.__setitem__(field, value)

            prop = self.properties.get(field, None)

            if prop:
                prop.changed = False

        return self

    def force_hydrate(self, **properties):
        for field, value in properties.items():
            prop = self.get_property(field, value)

            if not prop:
                continue

            reimmute = prop.immutable
            prop.immutable = False
            prop.value = value

            if reimmute:
                prop.immutable = True

        return self

    @property
    def changed(self):
        changed = {}

        for name, prop in self.properties.items():
            if prop.changed:
                changed[name] = {
                    'from': prop.original_value,
                    'to': prop.value,
                }

        return OrderedDict(sorted(changed.items()))

    def set_changed(self, changed=False):
        for name, prop in self.properties.items():
            prop.changed = changed
            prop.original_value = prop._value

        return self


class Property(object):
    default = None

    def __init__(self, value=None, data_type='python', default=None,
                 immutable=False, name=None):
        self.immutable = False
        self.name = name
        self._value = value
        self.original_value = value
        self._data_type = data_type
        self.data_type = data_type
        self.default = default or self.default
        self.immutable = immutable
        self.changed = False

    def _set_data_type(self, data_type):
        self._data_type = data_type

    def _get_data_type(self):
        return self._data_type

    data_type = property(_get_data_type, _set_data_type)

    def _get_value(self):
        if callable(self._value):
            value = self._value()
        else:
            value = self._value

        if not value and self.default:
            if callable(self.default):
                value = self.default()
            else:
                value = self.default

        if self.data_type == 'python':
            return self.to_python(value)

        return self.to_graph(value)

    def _set_value(self, value):
        if self.immutable:
            return

        self.changed = self.original_value != value
        self._value = value

    value = property(_get_value, _set_value)

    def to_python(self, value):
        return value

    def to_graph(self, value):
        return self.to_python(value)


class String(Property):

    def to_python(self, value):
        if not value:
            return ''

        return str(value)


class Integer(Property):
    default = 0

    def to_python(self, value):
        try:
            return int(float(value))
        except:
            return self.default


class Increment(Integer):

    def to_graph(self, value):
        self._value = self.to_python(value)
        self._value += 1

        return self._value


class Float(Property):
    default = 0.0

    def to_python(self, value):
        try:
            return float(value)
        except:
            return self.default


class Boolean(Property):
    default = False

    def _convert(self, value):
        if str(value).lower().strip() not in ['true', 'false']:
            value = bool(value)

        value = str(value).lower().strip()

        return bool(json.loads(value))

    def to_python(self, value):
        try:
            return self._convert(value)
        except:
            return False


class DateTime(Float):

    def to_python(self, value):
        if isinstance(value, datetime):
            value = value.timestamp()

        return super().to_python(value=value)


class TimeStamp(DateTime):

    def __init__(self, value=None, **kwargs):

        def default():
            return datetime.now()

        super().__init__(value=value, default=default, immutable=True)


class RelatedEntity(object):

    def __init__(self, relationship_entity=None, relationship_type=None,
                 direction='out', mapper=None, pagination_count=None):
        if not relationship_entity and not relationship_type:
            raise Exception()

        self.relationship_entity = relationship_entity
        self.relationship_type = relationship_type
        self.direction = direction
        self.results = None
        self._mapper = mapper
        self._limit = None
        self._skip = None
        self.relationship_query = RelationshipQuery(mapper=mapper,
            relationship_entity=relationship_entity,
            relationship_type=relationship_type, direction=direction)

    def reset(self):
        self._skip = None
        self._limit = None
        self.results = None
        self.relationship_query.reset()

        return self

    def __call__(self, limit=None, skip=None):
        unit = _Unit(entity=self.mapper.entity_context, action=self.query,
            mapper=self, limit=limit, skip=skip)

        return self.mapper.mapper.add_unit(unit).send()

    def _get_mapper(self):
        return self._mapper

    def _set_mapper(self, mapper):
        self._mapper = mapper
        self.relationship_query.mapper = mapper

        return self

    mapper = property(_get_mapper, _set_mapper)

    def skip(self, skip):
        self._skip = skip

        return self

    def limit(self, limit):
        self._limit = limit

        return self

    def connect(self, start, end, **properties):
        relationship_mapper = self.mapper.get_mapper(self.relationship_entity)
        relationship = relationship_mapper.create(**properties)
        relationship_mapper.connect(start=start, end=end,
            relationship_entity=relationship)

    def _traverse(self, limit=None, skip=None):
        query, params = self.query(limit=limit, skip=skip)

    def query(self, limit=None, skip=None, **kwargs):
        limit = limit or self._limit
        skip = skip or self._skip
        self.relationship_query.start_entity = self.mapper.entity_context
        self.relationship_query.skip = skip
        self.relationship_query.limit = limit

        return self.relationship_query.query()

    def next(self):
        return self

    def iter(self):
        return self