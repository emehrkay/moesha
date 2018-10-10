import copy
import json

from collections import OrderedDict
from datetime import datetime

from .query import RelatedEntityQuery


class PropertyManager(object):

    def __init__(self, properties=None, data_type='python',
                 allow_undefined=False):
        self._properties = {}
        self.properties = properties or {}
        self._data_type = data_type
        self.data_type = data_type
        self.allow_undefined = allow_undefined

    def reset(self):
        for f, p in self.properties.items():
            p.reset()

        return self

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
    def default_data(self):
        data = {n: f.value for n, f in self.properties.items()}

        return OrderedDict(sorted(data.items()))

    def data(self, properties=None):
        default = copy.copy(self.default_data)
        default.update(properties or {})
        data = {}

        for name, value in default.items():
            prop = self.get_property(name, value)

            if prop is not None:
                prop.value = value
                data[name] = prop.value

        self.reset()

        return data

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
            else:
                obj = Property

            prop = obj(value=value, data_type=self.data_type, name=field)

            if prop not in self.properties:
                self.properties[field] = prop

        return prop


class Property(object):
    default = None

    def __init__(self, value=None, data_type='python', default=None,
                 immutable=False, name=None, options=None):
        self.immutable = False
        self.name = name
        self._value = value
        self._original_value = value
        self._data_type = data_type
        self.data_type = data_type
        self.default = default or self.default
        self.immutable = immutable
        self.options = options or []

    def reset(self):
        if self._original_value is not None:
            self._value = self._original_value
        else:
            self._value = self.default

        return self

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

        if value is None and self.default:
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

        if options and value not in self.options:
            return

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
            return self.default


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


class RelatedManager(object):

    def __init__(self, mapper, relationships, allow_undefined=True):
        self._relationships = {}
        self.relationships = relationships or {}
        self.allow_undefined = allow_undefined
        self._mapper = None
        self.mapper = mapper

    def __call__(self, entity):
        for _, rel in self.relationships.items():
            rel.start_entity = entity

        return self

    def _get_relationships(self):
        return self._relationships

    def _set_relationships(self, relationships=None):
        self._relationships = relationships or {}

    relationships = property(_get_relationships, _set_relationships)

    def _get_mapper(self):
        return self._mapper

    def _set_mapper(self, mapper):
        for _, rel in self.relationships.items():
            rel.mapper = mapper

        return self

    mapper = property(_get_mapper, _set_mapper)

    def __getitem__(self, name):
        return self.get_relationship(name)

    def get_relationship(self, name):
        if name in self.relationships:
            return self.relationships[name]

        if self.allow_undefined:
            return RelatedEntity(mapper=self.mapper)

        return None


class RelatedEntity(object):

    def __init__(self, relationship_entity=None, relationship_type=None,
                 direction='out', mapper=None, ensure_unique=False):
        if not relationship_entity and not relationship_type:
            raise Exception()

        self.relationship_entity = relationship_entity
        self.relationship_type = relationship_type
        self.direction = direction
        self.ensure_unique = ensure_unique
        self._start_entity = None
        self.results = None
        self._mapper = mapper
        self._limit = None
        self._skip = None
        self._matches = []
        self._wheres = []
        self._orders = []
        self._returns = []
        self.relationship_query = RelatedEntityQuery(
            relationship_entity=None, direction=direction,
            relationship_type=relationship_type)

    def reset(self):
        self._skip = None
        self._limit = None
        self.results = None
        self._matches = []
        self._wheres = []
        self._orders = []
        self._returns = []
        self.relationship_query.reset()

        return self

    def __call__(self, return_relationship=False, limit=None, skip=None,
                 matches=None, wheres=None, orders=None, returns=None,
                 **kwargs):
        from .mapper import _Unit, Work

        work = Work(mapper=self.mapper.mapper)
        unit = _Unit(entity=self.mapper.entity_context, action=self.query,
            mapper=self, limit=limit, skip=skip, wheres=wheres, orders=orders,
            return_relationship=return_relationship)

        return work.add_unit(unit).send()

    def _get_mapper(self):
        return self._mapper

    def _set_mapper(self, mapper):
        self._mapper = mapper
        relationship = mapper.mapper.create(entity=self.relationship_entity)
        self.relationship_query.relationship_entity = relationship

        return self

    mapper = property(_get_mapper, _set_mapper)

    def _get_start_entity(self):
        return self._start_entity

    def _set_start_entity(self, start):
        self._start_entity = start
        self.relationship_query.start_entity = start

        return self

    start_entity = property(_get_start_entity, _set_start_entity)

    def match(self, *matches):
        for m in matches:
            self._matches.append(w)

        return self

    def where(self, *wheres):
        for w in wheres:
            self._wheres.append(w)

        return self

    def order(self, *orders):
        for o in orders:
            self._orders.append(o)

        return self

    def skip(self, skip):
        self._skip = skip

        return self

    def limit(self, limit):
        self._limit = limit

        return self

    def returns(self, *returns):
        self._returns = list(returns)

        return self

    def add(self, entity, properties=None, work=None):
        properties = properties or {}
        relationship = self.relationship_query.connect(entity=entity,
                properties=properties)

        self.mapper.mapper.save(relationship,
            ensure_unique=self.ensure_unique)

        return relationship

    def delete(self, entity):
        response = self.relationship_query.delete(entity)

        self.reset()

        return response

    def _traverse(self, limit=None, skip=None):
        query, params = self.query(limit=limit, skip=skip)

    def query(self, limit=None, skip=None, matches=None, wheres=None,
              orders=None, returns=None, return_relationship=False, **kwargs):
        limit = limit or self._limit
        skip = skip or self._skip
        matches = matches or self._matches
        wheres = wheres or self._wheres
        orders = orders or self._orders
        returns = returns or self._returns
        self.relationship_query.skip = skip
        self.relationship_query.limit = limit
        self.relationship_query.matches = self.relationship_query.matches \
            + matches
        self.relationship_query.wheres = self.relationship_query.wheres \
            + wheres
        self.relationship_query.orders = self.relationship_query.orders \
            + orders
        response = self.relationship_query.query(
            return_relationship=return_relationship, returns=returns)

        self.reset()

        return response
