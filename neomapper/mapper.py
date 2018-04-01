import copy

from functools import partial

from six import with_metaclass

from pypher.builder import Pypher, Params

from .entity import (Node, StructuredNode, Relationship,
    StructuredRelationship, get_entity)
from .query import Query
from .util import (entity_name)


GENERIC_MAPPER = 'generic.mapper'
ENTITY_MAPPER_MAP = {}
_MEMO = {}


def get_mapper(entity, mapper):
    name = entity_name(entity)

    if name in ENTITY_MAPPER_MAP:
        if name in _MEMO:
            _MEMO[name].mapper = mapper
            return _MEMO[name]

        mapper = ENTITY_MAPPER_MAP[name](mapper=mapper)
        _MEMO[name] = mapper

        return mapper

    if GENERIC_MAPPER in _MEMO:
        _MEMO[GENERIC_MAPPER].mapper = mapper
        return _MEMO[GENERIC_MAPPER]

    mapper = ENTITY_MAPPER_MAP[GENERIC_MAPPER](mapper=mapper)
    _MEMO[GENERIC_MAPPER] = mapper

    return mapper


class EntityQueryVariable(object):
    storage = {}

    @classmethod
    def define(cls, entity):
        if entity.query_variable:
            return entity.query_variable

        name = entity_name(entity)

        if name not in cls.storage:
            cls.storage[name] = 0

        var = 'n' if isinstance(entity, Node) else 'r'
        var = '{}{}'.format(var, cls.storage[name])
        cls.storage[name] += 1
        entity.query_variable = var

        return var

    @classmethod
    def reset(cls):
        cls.storage = {}


EQV = EntityQueryVariable


class _Unit(object):

    def __init__(self, entity, action, mapper):
        self.entity = entity
        self.action = action
        self.mapper = mapper


class _RootMapper(type):

    def __new__(cls, name, bases, attrs):
        relationships = {}

        for n, rel in attrs.items():
            if isinstance(rel, Relationship):
                relationships[n] = rel

        def _build_relationships(self):
            for name, rel in relationships.items():
                rel = copy.deepcopy(rel)
                rel.mapper = self
                rel.entity = self.entity

                setattr(self, name, rel)

        cls = super(_RootMapper, cls).__new__(cls, name, bases, attrs)
        entity = attrs.pop('entity', None)

        if entity:
            map_name = entity_name(entity)
            ENTITY_MAPPER_MAP[map_name] = cls
        elif name == 'EntityMapper':
            ENTITY_MAPPER_MAP[GENERIC_MAPPER] = cls

        setattr(cls, '_build_relationships_', _build_relationships)

        return cls


class EntityMapper(with_metaclass(_RootMapper)):

    def __init__(self, mapper=None):
        self.mapper = mapper
        self.before_events = []
        self.after_events = []
        self._build_relationships_()

    def reset(self):
        self.before_events = []
        self.after_events = []

    def create(self, entity=None, properties=None, label=None):
        properties = properties or {}

        if not entity:
            entity = get_entity(label)

        try:
            entity = entity(properties=properties)
        except Exception as e:
            raise e

        return entity

    def save(self, entity):
        exists = bool(entity.id)
        transaction = {
            'entity': entity,
            'mapper': self,
        }

        if isinstance(entity, Node):
            if exists:
                transaction['action'] = '_update_node'
                self.apply_update(entity)
            else:
                transaction['action'] = '_create_node'
                self.apply_create(entity)
        elif isinstance(entity, Relationship):
            if exists:
                transaction['action'] = '_update_node'
            else:
                start = entity.start
                end = entity.end

                if not isinstance(start, Node):
                    raise ArgumentException()

                if not isinstance(end, Node):
                    raise ArgumentException()

                EQV.define(start)
                EQV.define(end)

                start_mapper = get_mapper(entity=start, mapper=self.mapper)
                end_mapper = get_mapper(entity=end, mapper=self.mapper)

                if start.id:
                    start_mapper.apply_update(start)
                else:
                    start_mapper.apply_create(start)

                if end.id:
                    end_mapper.apply_update(end)
                else:
                    end_mapper.apply_create(end)

                self.before_events.extend(start_mapper.before_events)
                self.after_events.extend(start_mapper.after_events)
                start_mapper.reset()
                end_mapper.reset()

                transaction['action'] = '_create_relationship'
        else:
            raise ArgumentException('NOT ALLOWED')

        EQV.define(entity)
        unit = _Unit(**transaction)
        self.mapper.add_unit(unit)

        return self

    def delete(self, entity):
        pass

    def _create_node(self, entity):
        query = Query(entities=[entity,], params=self.mapper.params)

        return query.save()

    def _update_node(self, entity):
        query = Query(entities=[entity,], params=self.mapper.params)

        return query.save()

    def _create_relationship(self, entity):
        query = Query(entities=[entity,], params=self.mapper.params)

        return query.save()

    def _delete_node(self, entity):
        query = Query(entities=[entity,], params=self.mapper.params)

        return query.save()

    def _update_relationship(self, entity):
        query = Query(entities=[entity,], params=self.mapper.params)

        return query.save()

    def apply_create(self, entity):
        before = partial(self.on_before_save, entity=entity)
        after = partial(self.on_after_save, entity=entity)

        self.before_events.append(before)
        self.after_events.append(after)

        return self

    def apply_update(self, entity):
        before = partial(self.on_before_update, entity=entity)
        after = partial(self.on_after_update, entity=entity)

        self.before_events.append(before)
        self.after_events.append(after)

        return self

    def apply_delete(self, entity):
        before = partial(self.on_before_delete, entity=entity)
        after = partial(self.on_after_delete, entity=entity)

        self.before_events.append(before)
        self.after_events.append(after)

        return self

    def on_before_save(self, entity):
        pass

    def on_after_save(self, entity):
        pass

    def on_before_update(self, entity):
        pass

    def on_after_update(self, entity):
        pass

    def on_before_delete(self, entity):
        pass

    def on_after_delete(self, entity):
        pass


class Mapper(object):
    PARAM_PREFIX = '$NM'

    def __init__(self, connection):
        self.connection = connection
        self.params = Params(self.PARAM_PREFIX)
        self.units = []

    def reset(self):
        from .query import _ValueManager

        self.units = []

        _ValueManager.reset()
        EQV.reset()
        self.params.reset()

    def add_unit(self, unit):
        self.units.append(unit)

        return self

    def get_mapper(self, entity):
        return get_mapper(entity=entity, mapper=self)

    def save(self, entity):
        mapper = self.get_mapper(entity)

        return mapper.save(entity)

    def delete(self, entity):
        mapper = self.get_mapper(entity=entity)

        return mapper.delete(entity)

    def create(self, entity=None, properties=None, label=None):
        mapper = self.get_mapper(entity=entity)

        return mapper.create(entity=entity, properties=properties, label=label)

    def prepare(self):
        queries = []
        params = {}

        for unit in self.units:
            query, param = getattr(unit.mapper, unit.action)(unit.entity)
            queries.append(query)
            params.update(param)

        return queries, params

    def send(self):
        pass

    def query(self, pypher=None, query=None, params=None):
        if pypher:
            query = str(pypher)
            params = pypher.bound_params
        elif query:
            params = params or {}


class Related(object):

    def __init__(self, other_entity, relationship_entity=None,
                 direction='out'):
        self.other_entity = other_entity
        self.relationship_entity = relationship_entity
        self.direction = direction
        self.results = None
        self.mapper = None
        self.pypher = Pypher()

    def __call__(self, entity=None):
        return self._traverse()

    def _traverse(self, entity=None):
        pypher = self.pypher
        pypher.node()

        if self.relationship_entity:
            pypher.rel(label=self.relationship_entity.label,
                direction=self.direction)
        else:
            pypher.rel(direction=self.direction)

        pypher.node(label=self.other_entity.label)
        self.results = self.mapper.query(self.query)

    def query(self):
        return self.pypher


    def next(self):
        return self

    def iter(self):
        return self
