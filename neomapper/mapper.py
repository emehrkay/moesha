import copy

from functools import partial

from six import with_metaclass

from pypher.builder import Pypher, Params

from .entity import (Node, StructuredNode, Relationship,
    StructuredRelationship, get_entity)
from .query import (Query, RelationshipQuery)
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
        if hasattr(entity, 'query_variable') and entity.query_variable:
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


class RelatedManager(object):

    def __init__(self, mapper, relationships, allow_undefined=True):
        self._relationships = {}
        self.relationships = relationships or {}
        self.allow_undefined = allow_undefined

    def __call__(self, entity):
        for _, rel in self._relationships:
            rel.start_entity = entity

    def _get_relationships(self):
        return self._relationships

    def _set_relationships(self, relationships=None):
        self._relationships = relationships or {}

    def __getitem__(self, name):
        self.get_relationship(name)

    def get_relationship(self, name):
        if name in self.relationships:
            return self.relationships[name]

        if self.allow_undefined:
            return Related(mapper=self.mapper)


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
        return self._traverse(limit=limit, skip=skip)

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

    def query(self, limit=None, skip=None):
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


class _Unit(object):

    def __init__(self, entity, action, mapper, **kwargs):
        self.entity = entity
        self.action = action
        self.mapper = mapper
        self.kwargs = kwargs


class _RootMapper(type):

    def __new__(cls, name, bases, attrs):
        relationships = {}

        for n, rel in attrs.items():
            if isinstance(rel, RelatedEntity):
                relationships[n] = rel

        def _build_relationships(self):
            for name, rel in relationships.items():
                rel = copy.deepcopy(rel)
                rel.mapper = self
                rel.entity = self.entity

                setattr(self, name, rel)

            self.relationships = RelatedManager(mapper=self,
                relationships=relationships)

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
    entity = None

    def __init__(self, mapper=None):
        self.mapper = mapper
        self.before_events = []
        self.after_events = []
        self._build_relationships_()
        self._property_changes = {}
        self._entity_context = None

    def reset(self):
        self.before_events = []
        self.after_events = []
        self._entity_context = None

    def __call__(self, entity=None):
        self.entity_context = entity
        self.relationships(entity)

        return self

    def _get_entity_context(self):
        return self._entity_context or entity

    def _set_entity_context(self, entity):
        self._entity_context = entity

    def _delete_entity_context(self):
        self._entity_context = None

    entity_context = property(_get_entity_context, _set_entity_context,
        _delete_entity_context)

    def create(self, id=None, entity=None, properties=None, label=None):
        properties = properties or {}

        if not entity:
            if label:
                entity = get_entity(label)
            else:
                entity = Node

        if label and not entity:
            entity = get_entity(label)

        try:
            entity = entity(id=id, properties=properties)
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

    def delete(self, entity, detach=True):
        self.apply_delete(entity)
        unit = _Unit(entity=entity, action='_delete_entity', mapper=self,
            detach=detach)

        self.mapper.add_unit(unit)

        return self

    def _create_node(self, entity):
        query = Query(entities=[entity,], params=self.mapper.params)

        return query.save()

    def _update_node(self, entity):
        query = Query(entities=[entity,], params=self.mapper.params)

        return query.save()

    def _create_relationship(self, entity):
        query = Query(entities=[entity,], params=self.mapper.params)

        return query.save()

    def _delete_entity(self, entity, detach=True):
        query = Query(entities=[entity,], params=self.mapper.params)

        return query.delete(detach=detach)

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

    def on_after_save(self, entity, response=None):
        pass

    def on_before_update(self, entity):
        pass

    def on_after_update(self, entity, response=None):
        pass

    def on_before_delete(self, entity):
        pass

    def on_after_delete(self, entity, response=None):
        pass


class Mapper(object):
    PARAM_PREFIX = '$NM'

    def __init__(self, connection=None):
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

    def delete(self, entity, detach=True):
        mapper = self.get_mapper(entity=entity)

        return mapper.delete(entity, detach=detach)

    def create(self, id=None, entity=None, properties=None, label=None):
        mapper = self.get_mapper(entity=entity)

        return mapper.create(id=id, entity=entity, properties=properties,
            label=label)

    def prepare(self):
        queries = []
        params = {}

        for unit in self.units:
            kwargs = unit.kwargs
            kwargs['entity'] = unit.entity
            query, param = getattr(unit.mapper, unit.action)(**kwargs)

            queries.append(query)
            params.update(param)

        return queries, params

    def send(self):
        queries, params = self.prepare()
        response = [] #@TODO: build a real response object

        self._execute_before()

        for query in queries:
            self.query(query=query, params=params)

        self._execute_after()

    def query(self, pypher=None, query=None, params=None):
        if pypher:
            query = str(pypher)
            params = pypher.bound_params
        elif query:
            params = params or {}

    def _execute_before(self):
        for unit in self.units:
            for before in unit.mapper.before_events:
                before()

    def _execute_after(self, response=None):
        for unit in self.units:
            for after in unit.mapper.after_events:
                after(response=response)
