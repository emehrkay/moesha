import copy

from functools import partial

from six import with_metaclass

from pypher.builder import Pypher, Params

from neo4j import v1

from .entity import (Node, StructuredNode, Relationship,
    StructuredRelationship, get_entity, Collection)
from .query import (Query, RelationshipQuery, Helpers)
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
    counts = {
        'n': 0,
        'r': 0,
    }

    @classmethod
    def define(cls, entity):
        if hasattr(entity, 'query_variable') and entity.query_variable:
            return entity.query_variable

        t_var = 'n' if isinstance(entity, Node) else 'r'
        var = '{}{}'.format(t_var, cls.counts[t_var])
        cls.counts[t_var] += 1
        entity.query_variable = var

        return var

    @classmethod
    def reset(cls):
        cls.counts = {
            'n': 0,
            'r': 0,
        }


EQV = EntityQueryVariable


class RelatedManager(object):

    def __init__(self, mapper, relationships, allow_undefined=True):
        self._relationships = {}
        self.relationships = relationships or {}
        self.allow_undefined = allow_undefined
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

    def __getitem__(self, name):
        self.get_relationship(name)

    def get_relationship(self, name):
        if name in self.relationships:
            return self.relationships[name]

        if self.allow_undefined:
            return RelatedEntity(mapper=self.mapper)


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
        unit = _Unit(entity=self.mapper.entity_context, action='query',
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


class _Unit(object):

    def __init__(self, entity, action, mapper, event_map=None, event=None,
                 **kwargs):
        self.entity = entity
        self.action = action
        self.mapper = mapper
        self.event_map = event_map or {}
        self._event = event
        self.kwargs = kwargs
        self.before_events = []
        self.after_events = []
        self.final_events = self.event_map.get('final', [])
        self.query = None
        self.params = None

    def __repr__(self):
        return ('<moesha.mapper._Unit at {} for entity: {} at {}>').format(
            id(self), entity_name(self.entity), id(self.entity))

    def _get_event(self):
        return self._event

    def _set_event(self, event):
        self._event = event

        if event in self.event_map:
            self.before_events = self.event_map[event]['before']
            self.after_events = self.event_map[event]['after']

    event = property(_get_event, _set_event)

    def prepare(self):
        kwargs = {'unit': self}
        kwargs.update(self.kwargs)

        self.query, self.params = getattr(self.mapper, self.action)(**kwargs)

    def execute_before_events(self):
        for event in self.before_events:
            event(self.entity)

    def execute_after_events(self, *args, **kwargs):
        for event in self.after_events:
            event(self.entity, *args, **kwargs)

    def execute_final_events(self):
        for event in self.final_events:
            event()


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
    CREATE = 'create'
    UPDATE = 'update'
    DELETE = 'delete'
    FINAL = 'final'

    def __init__(self, mapper=None):
        self.mapper = mapper
        self.before_events = []
        self.after_events = []
        self._entity_context = None
        self._build_relationships_()
        self._event_map = {
            self.CREATE: {
                'before': [self.on_before_create,],
                'after': [self._refresh_entity, self.on_after_create,],
            },
            self.UPDATE: {
                'before': [self.on_before_update,],
                'after': [self.on_after_update,],
            },
            self.DELETE: {
                'before': [self.on_before_delete,],
                'after': [self.on_after_delete,],
            },
            self.FINAL: [self.reset]
        }

    def reset(self, *args, **kwargs):
        self.before_events = []
        self.after_events = []
        self._entity_context = None

    def __call__(self, entity=None):
        self.entity_context = entity
        self.relationships(entity)

        return self

    def _get_entity_context(self):
        return self._entity_context

    def _set_entity_context(self, entity):
        self._entity_context = entity

    def _delete_entity_context(self):
        self._entity_context = None

    entity_context = property(_get_entity_context, _set_entity_context,
        _delete_entity_context)

    def create(self, id=None, entity=None, properties=None, label=None,
               start=None, end=None, entity_type='node'):
        properties = properties or {}

        if label and not entity:
            entity = get_entity(label)

        try:
            entity = entity(id=id, properties=properties)
        except:
            if entity_type == 'relationship':
                entity = Relationship(id=id, labels=label,
                    properties=properties, start=start, end=end)
            else:
                entity = Node(id=id, properties=properties, labels=label)

        if start:
            entity.start = start

        if end:
            entity.end = end

        return entity

    def save(self, entity):
        EQV.define(entity)

        unit = _Unit(entity=entity, action='_save_entity', mapper=self,
            event_map=self._event_map)
        self.mapper.add_unit(unit)

        return self

    def delete(self, entity, detach=True):
        unit = _Unit(entity=entity, action='_delete_entity', mapper=self,
            detach=detach, event_map=self._event_map)

        self.mapper.add_unit(unit)

        return self

    def _save_entity(self, unit):
        entity = unit.entity
        exists = bool(entity.id)

        if isinstance(entity, Node):
            if exists:
                unit.event = self.UPDATE
                return self._update_node(entity)
            else:
                unit.event = self.CREATE
                return self._create_node(entity)
        elif isinstance(entity, Relationship):
            if exists:
                unit.event = self.UPDATE
                return self._update_node(entity)
            else:
                """Tricky logic in this block. Since the main Mapper loops
                over _Unit objects and executes their before and after events
                in place and this code block is currently executing inside of
                _Unit, this needs to register before and after events for the
                start and end nodes. The before events will be executed
                immediately because the before events for the relationship
                entity have already been executed. The after events will be
                added to the _Unit's after_events"""
                start = entity.start
                end = entity.end

                if not isinstance(start, Node):
                    raise MapperException()

                if not isinstance(end, Node):
                    raise MapperException()

                EQV.define(start)
                EQV.define(end)

                start_mapper = get_mapper(entity=start, mapper=self.mapper)
                end_mapper = get_mapper(entity=end, mapper=self.mapper)

                if start.id is not None:
                    start_events = start_mapper._event_map[EntityMapper.UPDATE]
                else:
                    start_events = start_mapper._event_map[EntityMapper.CREATE]

                if end.id is not None:
                    end_events = end_mapper._event_map[EntityMapper.UPDATE]
                else:
                    end_events = end_mapper._event_map[EntityMapper.CREATE]

                # run all of the before events for the start and end nodes
                for event in start_events['before']:
                    event(start)

                for event in end_events['before']:
                    event(end)

                # build all of the after events for both start and end
                afters = []

                for event in start_events['after']:
                    def bind_event(start_event):
                        def after_event(entity, response=None):
                            start_event(start, response)

                        afters.append(after_event)

                    bind_event(event)

                for event in end_events['after']:
                    def bind_event(end_event):
                        def after_event(entity, response=None):
                            end_event(end, response)

                        afters.append(after_event)

                    bind_event(event)

                unit.after_events = afters + unit.after_events

                # add the finals
                unit.final_events = start_mapper._event_map['final'] \
                    + end_mapper._event_map['final'] + unit.final_events

                return self._create_relationship(entity)
        else:
            raise MapperException('NOT ALLOWED')

    def _create_node(self, entity):
        query = Query(entities=[entity,], params=self.mapper.params)

        return query.save()

    def _update_node(self, entity):
        query = Query(entities=[entity,], params=self.mapper.params)

        return query.save()

    def _create_relationship(self, entity):
        query = Query(entities=[entity,], params=self.mapper.params)

        return query.save()

    def _delete_entity(self, unit, detach=True):
        entity = unit.entity
        unit.event = EntityMapper.DELETE
        query = Query(entities=[entity,], params=self.mapper.params)

        return query.delete(detach=detach)

    def _update_relationship(self, entity):
        query = Query(entities=[entity,], params=self.mapper.params)

        return query.save()

    def _refresh_entity(self, entity, response):
        for res in response.response.result_data:
            for var, node in res.items():
                if var == entity.query_variable:
                    entity.query_variable = None
                    entity.id = node.id
                    properties = {k:v for k,v  in node.items()}

                    entity.hydrate(**properties)

    def on_before_create(self, entity):
        pass

    def on_after_create(self, entity, response=None):
        pass

    def on_before_update(self, entity):
        pass

    def on_after_update(self, entity, response=None):
        pass

    def on_before_delete(self, entity):
        pass

    def on_after_delete(self, entity, response=None):
        pass

    # Utility methods
    def get_by_id(self, id_val=None):
        '''This method will return an entity by id. It does not attempt to
        cast the resulting node into the type that the current EntityMapper
        handles, but will return whatever entity belongs to the id_val'''
        unit = _Unit(entity=self.entity(), action='_get_by_id', mapper=self,
            id_val=id_val, event_map=self._event_map)
        self.mapper.add_unit(unit)
        query, params = self.mapper.prepare()
        result = self.mapper.query(query=query[0], params=params)

        if len(result) > 1:
            err = ('There was more than one result for id: {}'.format(id_val))
            raise MapperException(err)

        return result[0] if len(result) else None

    def _get_by_id(self, entity, id_val=None):
        helpers = Helpers()

        return helpers.get_by_id(entity=entity, id_val=id_val)


class EntityNodeMapper(EntityMapper):
    entity = Node


class EntityRelationshipMapper(EntityMapper):
    entity = Relationship


class Mapper(object):
    PARAM_PREFIX = '$NM'

    def __init__(self, connection=None):
        self.connection = connection
        self.params = Params(self.PARAM_PREFIX)
        self.params = None
        self.units = []

    def reset(self):
        from .query import _ValueManager

        self.units = []

        _ValueManager.reset()
        EQV.reset()
        # self.params.reset()

    def entity_used(self, entity):
        for u in self.units:
            if u.entity == entity:
                return True

        return False

    def remove_entity_unit(self, entity):
        """This method will ensure that an entity only has one unit of work
        registered with the Mapper. It will also reset any matched
        EntityMapper if found"""
        index = None

        for i, u in enumerate(self.units):
            if u.entity == entity:
                index = i
                break

        if index is not None:
            self.units[i].mapper.reset()
            del self.units[i]

        return self

    def add_unit(self, unit):
        self.units.append(unit)

        return self

    def get_mapper(self, entity):
        return get_mapper(entity=entity, mapper=self)

    def save(self, entity):
        self.remove_entity_unit(entity)
        mapper = self.get_mapper(entity)

        return mapper.save(entity)

    def delete(self, entity, detach=True):
        mapper = self.get_mapper(entity=entity)

        return mapper.delete(entity, detach=detach)

    def create(self, id=None, entity=None, properties=None, label=None,
               entity_type='node', start=None, end=None):
        mapper = self.get_mapper(entity=entity)

        return mapper.create(id=id, entity=entity, properties=properties,
            label=label, entity_type=entity_type, start=start, end=end)

    def get_by_id(self, entity=None, id_val=None):
        entity = entity or Node
        mapper = self.get_mapper(entity=entity)

        return mapper.get_by_id(id_val=id_val)

    def send(self):
        response = Response(mapper=self)

        """each unit will be processed, its before events executed, then the
        actual query will be run, and the after and final events will be run.
        If the unit's entity is a relationship, the start and end entities
        will have their before events run instantly and their after and final 
        events appended to the unit's"""
        for unit in self.units:
            unit.prepare()
            unit.execute_before_events()

            resp = self.query(query=unit.query, params=unit.params)

            unit.execute_after_events(response=resp)
            unit.execute_final_events()

            response += resp.data

        self.reset()

        return response

    def query(self, pypher=None, query=None, params=None):
        if pypher:
            query = str(pypher)
            params = pypher.bound_params

        from .util import _query_debug
        print(_query_debug(query, params))
        params = params or {}
        res = self.connection.query(query=query, params=params)
        response = Response(mapper=self, response=res)

        return response

    def queries(self):
        queries = []

        for unit in self.units:
            unit.prepare()
            queries.append((unit.query, unit.params,))

        return queries


class Response(Collection):

    def __init__(self, mapper, response=None):
        self.mapper = mapper
        self.response = response
        super(Response, self).__init__()

    @property
    def data(self):
        try:
            return self.response.data
        except:
            return []

    def __len__(self):
        return len(self.data)

    def __getitem__(self, key):
        try:
            return self.entities[key]
        except Exception as e:
            try:
                data = self.data[key]
                start = None
                end = None
                entity_type = 'node'

                if isinstance(data, v1.Relationship):
                    entity_type = 'relationship'
                    start = data.start
                    end = data.end

                entity = self.mapper.create(id=data.id,
                    properties=data.properties, entity_type=entity_type,
                    start=start, end=end)

                self.entities.append(entity)

                return entity
            except Exception as e:
                self.index = 0
                raise StopIteration()

    def __iadd__(self, other):
        if isinstance(other, Response):
            self.data.extend(other.data)
        else:
            self.data.extend(other)

        return self


class MapperException(Exception):
    pass
