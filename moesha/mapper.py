import copy
import functools
import logging

from functools import partial

from six import with_metaclass

from pypher.builder import Pypher, Params
from pypher.partial import Partial

from neo4j.v1 import types

from neobolt.exceptions import ConstraintError

from .entity import Node, Relationship, Collection
from .property import PropertyManager
from .query import (Builder, Query, RelationshipQuery, Helpers)
from .util import normalize_labels, entity_name, entity_to_labels


LOG = logging.getLogger(__name__)
NODE = 'node'
RELATIONSHIP = 'relationship'
GENERIC_MAPPER = 'generic.mapper'
ENTITY_MAPPER_MAP = {}
_MEMO = {}
ENTITY_MAP = {}


def get_entity(label=None):
    label = label or []

    if not isinstance(label, (list, set, tuple)):
        label = [label,]

    label = normalize_labels(*label)

    if label in ENTITY_MAP:
        return ENTITY_MAP[label]

    return Node


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

        self.query, self.params = self.action(**kwargs)

    def execute_before_events(self):
        for event in self.before_events:
            event(self.entity)

    def execute_after_events(self, *args, **kwargs):
        for event in self.after_events:
            event(self.entity, *args, **kwargs)

    def execute_final_events(self):
        for event in self.final_events:
            event()


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
        if name not in self.relationships and self.allow_undefined:
            self.relationships[name] = RelatedEntity()
        else:
            raise Exception('')

        return self.relationships[name]

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


class _RootMapper(type):

    def __new__(cls, name, bases, attrs):
        relationships = {}
        properties = {}
        glob = {
            'undefined_props': attrs.get('__ALLOW_UNDEFINED_PROPERTIES__',
                True),
            'undefined_rels': attrs.get('__ALLOW_UNDEFINED_RELATIONSHIPS__',
                True),
        }

        def get_props(source):
            props = source.get('__PROPERTIES__', {})
            rels = source.get('__RELATIONSHIPS__', {})

            properties.update(props)
            relationships.update(rels)

        def walk(bases):
            walk_bases = list(bases)

            walk_bases.reverse()

            for wb in walk_bases:
                walk(wb.__bases__)

            [get_props(b.__dict__) for b in walk_bases]

        walk(bases)
        get_props(attrs)

        for n, rel in attrs.get('__RELATIONSHIPS__', []):
            if isinstance(rel, RelatedEntity):
                relationships[n] = rel

        def __build__(self):
            self.properties = PropertyManager(properties=properties,
                allow_undefined=glob['undefined_props'], data_type='python')
            self.relationships = RelatedManager(mapper=self,
                relationships=relationships,
                allow_undefined=glob['undefined_rels'])

        def _build_relationships(self):
            for name, rel in relationships.items():
                rel = copy.deepcopy(rel)
                rel.mapper = self
                rel.entity = self.entity

                setattr(self, name, rel)

            self.relationships = {}
            # self.relationships = RelatedManager(mapper=self,
            #     relationships=relationships)

        cls = super(_RootMapper, cls).__new__(cls, name, bases, attrs)
        entity = attrs.pop('entity', None)

        if entity:
            map_name = entity_name(entity)
            ENTITY_MAPPER_MAP[map_name] = cls

            labels = attrs.get('__LABELS__', attrs.get('__TYPE__', None))

            if labels:
                if not isinstance(labels, (list, tuple, set)):
                    labels = [labels,]

                labels = normalize_labels(*labels)
            else:
                labels = entity_to_labels(entity)

            ENTITY_MAP[labels] = entity
        elif name == 'EntityMapper':
            ENTITY_MAPPER_MAP[GENERIC_MAPPER] = cls

        setattr(cls, '__build__', __build__)

        return cls


class EntityMapper(with_metaclass(_RootMapper)):
    entity = None
    CREATE = 'create'
    UPDATE = 'update'
    DELETE = 'delete'
    FINAL = 'final'
    __PROPERTIES__ = {}
    __RELATIONSHIPS__ = {}
    __PRIMARY_KEY__ = 'id'
    __ALLOW_UNDEFINED_PROPERTIES__ = True
    __ALLOW_UNDEFINED_RELATIONSHIPS__ = True
    __PROPERTY_MAPPINGS__ = {}

    def __init__(self, mapper=None):
        self.mapper = mapper
        self.before_events = []
        self.after_events = []
        self._entity_context = None
        self.__build__()
        self._property_change_handlers = {}
        self._event_map = {
            self.CREATE: {
                'before': [self.on_before_create,],
                'after': [self._refresh_entity, self.on_after_create,],
            },
            self.UPDATE: {
                'before': [self.on_before_update,],
                'after': [self.on_properties_changed, self.on_after_update,],
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

    def __getitem__(self, relationship_name):
        return self.relationships[relationship_name]

    def _get_data_type(self):
        return self.properties.data_type

    def _set_data_type(self, data_type):
        self.properties.data_type = data_type

        return self

    data_type = property(_get_data_type, _set_data_type)

    def _get_entity_context(self):
        return self._entity_context

    def _set_entity_context(self, entity):
        self._entity_context = entity

    def _delete_entity_context(self):
        self._entity_context = None

    entity_context = property(_get_entity_context, _set_entity_context,
        _delete_entity_context)

    def data(self, entity):
        return entity.data

    def entity_data(self, entity_data=None, data_type='python'):
        self.properties.data_type = data_type or self.data_type

        return self.properties.data(entity_data)

    def create(self, id=None, entity=None, properties=None, labels=None,
               start=None, end=None, entity_type=NODE, data_type='python'):
        properties = self.entity_data(properties or {}, data_type=data_type)

        if labels and not entity:
            entity = get_entity(labels)

        if not entity:
            entity = self.entity

        try:
            entity = entity(id=id, properties=properties, labels=labels)
        except:
            if entity_type == RELATIONSHIP:
                entity = Relationship(id=id, labels=labels,
                    properties=properties, start=start, end=end)
            else:
                entity = Node(id=id, properties=properties, labels=labels)

        if start:
            entity.start = start

        if end:
            entity.end = end

        return entity

    def save(self, entity):
        EQV.define(entity)

        unit = _Unit(entity=entity, action=self._save_entity, mapper=self,
            event_map=self._event_map)
        self.mapper.add_unit(unit)

        return self

    def delete(self, entity, detach=True):
        unit = _Unit(entity=entity, action=self._delete_entity, mapper=self,
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

                    entity.hydrate(properties=properties, reset=True)

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

    def on_properties_changed(self, entity, response=None):
        """This method checkes for changes in the entity's properties and will
        run a method that will handle what should happen if it changed.
        for most properties, naming a method with the format
        `on_$property_changed` should be fine. But for property names that 
        would not translate to a Python function name, the EntityMapper must
        modify the _property_change_handlers attribute where the key is the
        property name and the value is the name of the method that will handle
        it"""
        changes = entity.changes

        for field, values in changes.items():
            method = self._property_change_handlers.get(field, None)

            if not method:
                method_name = 'on_{}_property_changed'.format(field)

                if hasattr(self, method_name):
                    method = getattr(self, method_name)

            if method:
                method(entity=entity, field=field,
                    value_from=values['from'], value_to=values['to'])

    # Utility methods
    def get_by_id(self, id_val=None):
        def _get_by_id(unit, id_val=None):
            helpers = Helpers()

            return helpers.get_by_id(entity=unit.entity, id_val=id_val)

        unit = _Unit(entity=self.entity(), action=_get_by_id, mapper=self,
            id_val=id_val, event_map=self._event_map)
        self.mapper.add_unit(unit)
        result = self.mapper.send()

        if len(result) > 1:
            err = ('There was more than one result for id: {}'.format(id_val))
            raise MapperException(err)

        return result[0] if len(result) else None

    def builder(self, entity=None):
        return Builder(entity or self.entity())


class StructuredEntityMapper(EntityMapper):
    __ALLOW_UNDEFINED_PROPERTIES__ = False
    __ALLOW_UNDEFINED_RELATIONSHIPS__ = False


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

    def data(self, entity):
        mapper = self.get_mapper(entity)

        return mapper.data(entity)

    def save(self, *entities):
        for entity in entities:
            self.remove_entity_unit(entity)
            mapper = self.get_mapper(entity)

            mapper.save(entity)

        return self

    def delete(self, entity, detach=True):
        mapper = self.get_mapper(entity=entity)

        return mapper.delete(entity, detach=detach)

    def create(self, id=None, entity=None, properties=None, labels=None,
               entity_type=NODE, start=None, end=None, data_type='python'):
        if labels and not entity:
            entity = get_entity(labels)

        mapper = self.get_mapper(entity=entity)

        return mapper.create(id=id, entity=entity, properties=properties,
            labels=labels, entity_type=entity_type, start=start, end=end,
            data_type=data_type)

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
        try:
            for unit in self.units:
                unit.prepare()
                unit.execute_before_events()

                resp = self.query(query=unit.query, params=unit.params)

                unit.execute_after_events(response=resp)
                unit.execute_final_events()

                response += resp.data
        except Exception as e:
            raise e
        else:
            return response
        finally:
            self.reset()

    def query(self, pypher=None, query=None, params=None):
        if pypher:
            if isinstance(pypher, Partial):
                pypher.build()
                pypher = pypher.pypher

            query = str(pypher)
            params = pypher.bound_params

        from .util import _query_debug
        LOG.debug(query, params)
        LOG.debug(_query_debug(query, params))

        try:
            params = params or {}
            res = self.connection.query(query=query, params=params)
            response = Response(mapper=self, response=res)

            return response
        except ConstraintError as ce:
            raise MapperConstraintError(ce.message)
        except Exception as e:
            raise e

    def queries(self):
        queries = []

        for unit in self.units:
            unit.prepare()
            queries.append((unit.query, unit.params,))

        return queries

    def builder(self, entity):
        mapper = self.get_mapper(entity)

        return mapper.builder(entity)


class EntityNodeMapper(EntityMapper):
    entity = Node


class EntityRelationshipMapper(EntityMapper):
    entity = Relationship


class Response(Collection):

    def __init__(self, mapper, response=None):
        self.mapper = mapper
        self.response = response
        self._data = response.data if response else []
        super(Response, self).__init__()

    def _get_data(self):
        return self._data

    def _set_data(self, data):
        self._data = data

    data = property(_get_data, _set_data)

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
                entity_type = NODE
                labels = data.labels

                if isinstance(data, types.Relationship):
                    entity_type = RELATIONSHIP
                    start = data.start
                    end = data.end
                    labels = data.type

                entity = self.mapper.create(id=data.id, labels=list(labels),
                    properties=data._properties, entity_type=entity_type,
                    start=start, end=end)

                self.entities.append(entity)

                return entity
            except Exception as e:
                self.index = 0
                raise StopIteration(e)

    def __iadd__(self, other):
        if isinstance(other, Response):
            self.data.extend(other.data)
        else:
            self.data.extend(other)

        return self


class MapperException(Exception):

    def __init__(self, message):
        self.message = message


class MapperConstraintError(MapperException):

    @property
    def data(self):
        import re

        r = re.compile("(\w+)\((\d+)\) already exists with label `(.*)` and property `(.*)` = '(.*)'")
        m = r.findall(self.message)[0]

        return {
            'entity': m[0],
            'id': m[1],
            'labels': m[2],
            'field': m[3],
            'value': m[4],
        }
