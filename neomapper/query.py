from pypher import (Pypher, Param, __)

from .entity import (Node, Relationship, Collection)
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


class Query(object):

    def __init__(self, entities, params=None):
        if not isinstance(entities, (Collection, list, set, tuple)):
            entities = [entities,]

        self.entities = entities
        self.pypher = Pypher(params=params)
        self.creates = []
        self.matches = []
        self.deletes = []
        self.matched_entities = []
        self.sets = []
        self.returns = []

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
            rel.rel(entity.query_variable, labels=entity.label, direction='out')
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

    def _node_by_id(self, entity):
        qv = entity.query_variable
        _id = VM.get_next(entity, 'id')
        _id = Param(_id, entity.id)

        return __.node(qv).WHERE(__.ID(qv) == _id)
