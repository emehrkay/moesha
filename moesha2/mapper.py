import copy

from functools import partial

from six import with_metaclass

from pypher.builder import Pypher, Params

from neo4j.v1 import types

from .entity import Node, Relationship
from .property import PropertyManager
from .util import normalize_labels, entity_name, entity_to_labels


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


class _RootMapper(type):

    def __new__(cls, name, bases, attrs):
        relationships = {}
        properties = {}
        glob = {'allow_undefined': attrs.get('__ALLOW_UNDEFINED__', False)}

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
            props = {n: copy.deepcopy(p) for n, p in properties.items()}
            self.properties = PropertyManager(properties=props,
                allow_undefined=glob['allow_undefined'], data_type='python')

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
    __ALLOW_UNDEFINED__ = False
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

    def entity_data(self, entity_data=None, data_type='python'):
        self.properties.data_type = data_type or self.data_type

        return self.properties.data(entity_data)

    def create(self, id=None, entity=None, properties=None, label=None,
               start=None, end=None, entity_type='node', data_type='python'):
        properties = self.entity_data(properties or {}, data_type=data_type)

        if label and not entity:
            entity = get_entity(label)

        if not entity:
            entity = self.entity

        try:
            entity = entity(id=id, properties=properties)
        except:
            if entity_type == 'relationship':
                entity = Relationship(id=id,
                    properties=properties, start=start, end=end)
            else:
                entity = Node(id=id, properties=properties)

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

    def on_properties_changed(self, entity, response=None):
        """This method checkes for changes in the entity's properties and will
        run a method that will handle what should happen if it changed.
        for most properties, naming a method with the format
        `on_$property_changed` should be fine. But for property names that 
        would not translate to a Python function name, the EntityMapper must
        modify the _property_change_handlers attribute where the key is the
        property name and the value is the name of the method that will handle
        it"""
        changes = entity.changed

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

        unit = _Unit(entity=self.entity, action=_get_by_id, mapper=self,
            id_val=id_val, event_map=self._event_map)
        self.mapper.add_unit(unit)
        result = self.mapper.send()

        if len(result) > 1:
            err = ('There was more than one result for id: {}'.format(id_val))
            raise MapperException(err)

        return result[0] if len(result) else None


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

    def create(self, id=None, entity=None, properties=None, label=None,
               entity_type='node', start=None, end=None, data_type='python'):
        if label and not entity:
            entity = get_entity(label)

        mapper = self.get_mapper(entity=entity)

        return mapper.create(id=id, entity=entity, properties=properties,
            label=label, entity_type=entity_type, start=start, end=end,
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
