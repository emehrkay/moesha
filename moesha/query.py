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

    def __init__(self, params=None):
        self.creates = []
        self.matches = []
        self.deletes = []
        self.matched_entities = []
        self.sets = []
        self.wheres = []
        self.returns = []
        self.pypher = Pypher(params=params)
        self.matched_entities = []
        self.wheres_entities = []
        self.returns_entities = []

    def _node_by_id(self, entity):
        qv = entity.query_variable

        if not qv:
            qv = VM.set_query_var(entity)

        _id = VM.get_next(entity, 'id')
        _id = Param(_id, entity.id)

        return __.node(qv).WHERE(__.ID(qv) == _id)

    def _entity_by_id_builder(self, entity):
        qv = entity.query_variable

        if not qv:
            qv = VM.set_query_var(entity)

        _id = VM.get_next(entity, 'id')
        _id = Param(_id, entity.id)

        if isinstance(entity, Relationship):
            node = __.node().relationship(qv).node()
        else:
            node = __.node(qv)

        where = __.ID(qv) == _id

        self.matches.append(node)

        if entity not in self.wheres_entities:
            self.wheres.append(where)

            self.wheres_entities.append(entity)

        if entity not in self.returns_entities:
            self.returns.append(entity.query_variable)

            self.returns_entities.append(entity)

        return self

    def where_builder(self):
        wb = __()

        for i, where in enumerate(self.wheres):
            if i == 0:
                wb = where
            else:
                wb.AND(where)

        return wb


class Query(_BaseQuery):

    def __init__(self, entities, params=None):
        super(Query, self).__init__(params=params)

        if not isinstance(entities, (Collection, list, set, tuple)):
            entities = [entities,]

        self.entities = entities

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
            pypher.MATCH(*self.matches)

        if self.creates:
            pypher.CREATE(*self.creates)

        if self.sets:
            pypher.SET(*self.sets)

        if self.wheres:
            wb = self.where_builder()

            self.pypher.WHERE(wb)

        pypher.RETURN(*self.returns)

        return str(pypher), pypher.bound_params

    def create_node(self, entity):
        props = self._properties(entity)
        create = __.node(entity.query_variable, labels=entity.label, **props)

        self.creates.append(create)

        if entity not in self.returns_entities:
            self.returns.append(entity.query_variable)

            self.returns_entities.append(entity)

        return self

    def update_node(self, entity):
        props = self._properties(entity)
        qv = entity.query_variable

        if entity not in self.matched_entities:
            for field, value in props.items():
                stmt = getattr(__, qv).property(field)._
                stmt == value
                self.sets.append(stmt)

            self.matched_entities.append(entity)

        self._entity_by_id_builder(entity)

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

        """build final query could be a combination of queries:
        @todo: document potential queries
        """
        rel = __()

        if start.id:
            self.update_node(start)
            self.matches.pop()
            rel.node(start.query_variable)
        else:
            self.create_node(start)
            rel = self.creates.pop()

        if end.id:
            self.update_node(end)
            self.matches.pop()
        else:
            self.create_node(end)

        if entity.id:
            _id = VM.get_next(entity, 'id')
            _id = Param(_id, entity.id)
            rel.rel(entity.query_variable, labels=entity.label,
                direction='out')

            self.wheres.append(__.ID(entity.query_variable) == _id)
            self._update_properties(entity)
            self.matches.append(rel)
        else:
            rel.rel(entity.query_variable, labels=entity.label,
                direction='out', **props)
            self.creates.append(rel)

        if end.id:
            rel.node(end.query_variable)
        else:
            rel.append(self.creates.pop(0))

        if entity not in self.returns_entities:
            self.returns.append(entity.query_variable)

            self.returns_entities.append(entity)

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


class Helpers(_BaseQuery):

    def get_by_id(self, entity, id_val=None):
        '''This method is used to build a query that will return an entity
        --Node, Relationship-- by its id. It will create a query that looks
        like:

            MATCH ()-[r0]-() WHERE id(r0) = $r0_id_0 RETURN DISTINCT r

        for relationships OR for nodes

            MATCH (n0) WHERE id(n0) = $n0_id_0 RETURN DISTINCT n0
        '''
        if not entity.id and id_val:
            entity.id = id_val

        self._entity_by_id_builder(entity)

        self.pypher.MATCH(*self.matches)

        wb = self.where_builder()

        self.pypher.WHERE(wb)

        returns = map(__.DISTINCT, self.returns)
        self.pypher.RETURN(*returns)

        return str(self.pypher), self.pypher.bound_params


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

            self.pypher.MATCH.NODE(qv)
            self.wheres.append(where)
        else:
            self.pypher.MATCH.NODE(labels=self.start_entity.labels)

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
            wb = self.where_builder()

            self.pypher.WHERE(wb)

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
