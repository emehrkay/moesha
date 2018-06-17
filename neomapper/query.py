import uuid

from pypher import (Pypher, Param, __)

from .entity import (Entity, Node, Relationship, Collection)
from .util import normalize


class _ValueManager(object):
    values = {}

    @classmethod
    def reset(cls):
        cls.values = {}

    @classmethod
    def set_query_var(cls, entity):
        from .mapper import EntityQueryVariable

        return EntityQueryVariable.define(entity)

    @classmethod
    def get_next(cls, entity, field):
        entity_name = entity.__class__.__name__
        name = cls.set_query_var(entity)
        field = normalize(field)

        if entity_name not in _ValueManager.values:
            _ValueManager.values[entity_name] = 0

        return '${}_{}_{}'.format(name, field,
            _ValueManager.values[entity_name]).lower()


VM = _ValueManager


class _BaseQuery(object):

    def __init__(self):
        self.creates = []
        self.matches = []
        self.deletes = []
        self.matched_entities = []
        self.sets = []
        self.wheres = []
        self.returns = []

    def _node_by_id(self, entity):
        qv = entity.query_variable

        if not qv:
            qv = VM.set_query_var(entity)

        _id = VM.get_next(entity, 'id')
        _id = Param(_id, entity.id)

        return __.node(qv).WHERE(__.ID(qv) == _id)

    def _node_by_id_builder(self, entity):
        qv = entity.query_variable

        if not qv:
            qv = VM.set_query_var(entity)

        _id = VM.get_next(entity, 'id')
        _id = Param(_id, entity.id)
        node = __.node(qv)
        where = __.WHERE(__.ID(qv) == _id)

        self.matches.append(node)
        self.wheres.append(where)

        return self


class Query(_BaseQuery):

    def __init__(self, entities, params=None):
        super(Query, self).__init__()

        if not isinstance(entities, (Collection, list, set, tuple)):
            entities = [entities,]

        self.entities = entities
        self.pypher = Pypher(params=params)

    def save(self):
        for entity in self.entities:
            if isinstance(entity, Node):
                if entity.id:
                    self.update_node(entity)
                else:
                    self.create_node(entity)
            elif isinstance(entity, Relationship):
                self.save_relationship(entity)

        pypher = self.pypher

        if self.matches:
            for match in self.matches:
                pypher.MATCH(match)

        if self.creates:
            pypher.CREATE(*self.creates)

        if self.sets:
            pypher.SET(*self.sets)

        pypher.RETURN(*self.returns)

        return str(pypher), pypher.bound_params

    def create_node(self, entity):
        props = self._properties(entity)

        self.creates.append(__.node(entity.query_variable, **props))
        self.returns.append(entity.query_variable)

        return self

    def update_node(self, entity):
        props = self._properties(entity)
        qv = entity.query_variable

        for field, value in props.items():
            stmt = getattr(__, qv).property(field)._
            stmt == value
            self.sets.append(stmt)

        self.matches.append(self._node_by_id(entity))
        self.returns.append(qv)

        return self

    def save_relationship(self, entity):
        """this method handles creating and saving relationships because there
        are minor differences between the two"""
        start = entity.start
        start_properties = {}
        end = entity.end
        end_properties = {}
        props = self._properties(entity)

        if start is None or end is None:
            raise Exception('The relationship must have a start and end node')

        if not isinstance(start, Node):
            start = Node(id=start)

        if not isinstance(end, Node):
            end = Node(id=end)

        VM.set_query_var(start)
        VM.set_query_var(end)
        VM.set_query_var(entity)

        if start not in self.matched_entities:
            if start.id is not None:
                self._update_properties(start)
                self.matches.append(self._node_by_id(start))
            else:
                start_properties = self._properties(start)

            self.matched_entities.append(start)
            self.returns.append(start.query_variable)

        if end not in self.matched_entities:
            if end.id is not None:
                self._update_properties(end)
                self.matches.append(self._node_by_id(end))
            else:
                end_properties = self._properties(end)

            self.matched_entities.append(end)
            self.returns.append(end.query_variable)

        if entity.id is None:
            rel = __.node(start.query_variable, **start_properties)
            rel.rel(entity.query_variable, labels=entity.label,
                direction='out', **props)
            rel.node(end.query_variable, **end_properties)
            self.creates.append(rel)
        else:
            _id = VM.get_next(entity, 'id')
            _id = Param(_id, entity.id)
            rel = __.node(start.query_variable, **start_properties)
            rel.rel(entity.query_variable, labels=entity.label,
                direction='out')
            rel.node(end.query_variable, **end_properties)
            rel.WHERE(__.ID(entity.query_variable) == _id)
            self._update_properties(entity)
            self.matches.append(rel)

        self.returns.append(entity.query_variable)

        return self

    def delete(self, detach=False):
        for entity in self.entities:
            if not entity.id:
                continue

            if isinstance(entity, Node):
                self.delete_node(entity=entity)
            elif isinstance(entity, Relationship):
                self.delete_relationship(entity=entity)

                detach = False

        pypher = self.pypher

        if self.matches:
            for match in self.matches:
                pypher.MATCH(match)

        if detach:
            pypher.DETACH

        pypher.DELETE(*self.deletes)

        return str(pypher), pypher.bound_params

    def delete_node(self, entity):
        _id = VM.get_next(entity, 'id')
        _id = Param(_id, entity.id)
        match = __.node(entity.query_variable)
        match.WHERE(__.ID(entity.query_variable) == _id)

        self.matches.append(match)
        self.deletes.append(entity.query_variable)

        return self

    def delete_relationship(self, entity):
        _id = VM.get_next(entity, 'id')
        _id = Param(_id, entity.id)
        match = __.node()
        match.rel(entity.query_variable, labels=entity.label)
        match.node()
        match.WHERE(__.ID(entity.query_variable) == _id)

        self.matches.append(match)
        self.deletes.append(entity.query_variable)

        return self

    def _properties(self, entity):
        props = {}
        properties = entity.data

        for field, value in properties.items():
            name = VM.get_next(entity, field)
            param = Param(name=name, value=value)

            self.pypher.bind_param(param)

            props[field] = param

        return props

    def _update_properties(self, entity):
        props = self._properties(entity)
        qv = entity.query_variable

        for field, value in props.items():
            stmt = getattr(__, qv).property(field)._
            stmt == value
            self.sets.append(stmt)

        return self


class RelationshipQuery(_BaseQuery):

    def __init__(self, mapper, direction='out', relationship_entity=None,
                 relationship_type=None, relationship_prpoerties=None,
                 start_entity=None, end_entity=None, params=None,
                 single_relationship=False):
        super(RelationshipQuery, self).__init__()
        VM.set_query_var(relationship_entity)

        self.mapper = mapper
        self.direction = direction
        self.relationship_entity = relationship_entity
        self.relationship_type = relationship_type
        self._start_entity = None
        self.start_entity = start_entity
        self._end_entity = None
        self.end_entity = end_entity
        self.relationship_prpoerties = relationship_prpoerties or {}
        self.params = params
        self.skip = None
        self.limit = 1 if single_relationship else None
        self.other_end_key = 'node_' + str(uuid.uuid4())[-5:]

    def reset(self):
        self.skip = None
        self.limit = None

    def _get_start_entity(self):
        return self._start_entity

    def _set_start_entity(self, entity):
        if entity is not None and not isinstance(entity, Entity):
            raise

        if entity:
            VM.set_query_var(entity)

        self._start_entity = entity

    start_entity = property(_get_start_entity, _set_start_entity)

    def _get_end_entity(self):
        return self._end_entity

    def _set_end_entity(self, entity):
        if entity is not None and not isinstance(entity, Entity):
            raise

        if self._end_entity:
            VM.set_query_var(self._end_entity)

        self._end_entity = entity

    end_entity = property(_get_end_entity, _set_end_entity)

    def _build_start(self):
        if self.start_entity.id:
            qv = self.start_entity.query_variable
            where = __.ID(qv) == self.start_entity.id

            self.pypher.NODE(qv)
            self.wheres.append(where)
        else:
            self.pypher.NODE(labels=self.start_entity.labels)

        return self

    def _build_end(self):
        qv = self.other_end_key
        labels = None

        if self.end_entity:
            qv = self.end_entity.query_variable
            labels = self.end_entity.labels

        self.pypher.NODE(qv, labels=labels)
        self.returns.append(qv)

        return self

    def _build_relationship(self):
        if self.relationship_entity:
            self.pypher.relationship(direction=self.direction,
                labels=self.relationship_entity.labels,
                **self.relationship_prpoerties)
        else:
            self.pypher.relationship(direction=self.direction,
                labels=self.relationship_type)

        return self

    def query(self):
        self.start_entity = self.mapper.entity_context

        if not self.start_entity:
            raise RelatedQueryException(('Related objects must have a'
                ' start entity'))

        self.pypher = Pypher()
        pypher = self.pypher

        self._build_start()._build_relationship()._build_end()

        if self.wheres:
            self.pypher.WHERE(*self.wheres)

        self.pypher.RETURN(*self.returns)

        if self.skip is not None:
            self.pypher.SKIP(self.skip)

        if self.limit is not None:
            self.pypher.LIMIT(self.limit)

        return str(pypher), pypher.bound_params

    def connect(self, entity, properties=None):
        start = self.mapper.entity_context

        if not start:
            raise RelatedQueryException(('The relationship {} does not '))

        if self.relationship_entity:
            relationship = self.mapper.create(entity=self.relationship_entity,
                properties=properties)
        else:
            relationship = self.mapper.create(entity_type='relationship',
                properties=properties)

        relationship.start = start
        relationship.end = entity
        query = Query(entities=[relationship], params=self.params)

        return query.save()


class QueryException(Exception):
    pass


class RelatedQueryException(QueryException):
    pass
