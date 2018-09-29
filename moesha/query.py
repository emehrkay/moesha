import uuid

from pypher.builder import (Pypher, Param, Params, __)

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
        self.orders = []
        self.returns = []
        self.pypher = Pypher(params=params)

    def reset(self):
        self.creates = []
        self.matches = []
        self.deletes = []
        self.matched_entities = []
        self.sets = []
        self.wheres = []
        self.orders = []
        self.returns = []

    def _node_by_id(self, entity):
        qv = entity.query_variable

        if not qv:
            qv = VM.set_query_var(entity)

        _id = VM.get_next(entity, 'id')
        _id = Param(_id, entity.id)

        return __.node(qv).WHERE(__.ID(qv) == _id)

    def _entity_by_id_builder(self, entity, id_val, add_labels=False):
        qv = entity.query_variable

        if not qv:
            qv = VM.set_query_var(entity)

        _id = VM.get_next(entity, 'id')
        _id = Param(_id, id_val)
        node_kwargs = {}

        if add_labels:
            node_kwargs['labels'] = entity.labels

        if isinstance(entity, Relationship):
            node = __.node(**node_kwargs).relationship(qv).node()
        else:
            node = __.node(qv, **node_kwargs)

        where = __.ID(qv) == _id

        self.matches.append(node)
        self.wheres.append(where)
        self.returns.append(entity.query_variable)

        return self


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

        self.creates.append(__.node(entity.query_variable,
            labels=entity.labels, **props))
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

        rel = Pypher()

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
            if start.id is not None:
                rel = rel.node(start.query_variable)
            else:
                rel = rel.node(start.query_variable, labels=start.labels,
                    **start_properties)

            rel.rel(entity.query_variable, labels=entity.labels,
                direction='out', **props)

            if end.id is not None:
                rel.node(end.query_variable)
            else:
                rel.node(end.query_variable, labels=end.labels,
                    **end_properties)

            self.creates.append(rel)
        else:
            _id = VM.get_next(entity, 'id')
            _id = Param(_id, entity.id)

            if start.id is not None:
                rel = rel.node(start.query_variable)
            else:
                rel = rel.node(start.query_variable, labels=start.labels,
                    **start_properties)

            rel.rel(entity.query_variable, labels=entity.labels,
                direction='out')

            if end.id is not None:
                rel.node(end.query_variable)
            else:
                rel.node(end.query_variable, labels=end.labels,
                    **end_properties)

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
        match.rel(entity.query_variable, labels=entity.labels)
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


class RelatedEntityQuery(_BaseQuery):

    def __init__(self, direction='out', relationship_entity=None,
                 relationship_type=None, relationship_prpoerties=None,
                 start_entity=None, end_entity=None, params=None,
                 single_relationship=False, start_query_variable='start',
                 relationship_query_variable='relt', end_query_variable='end_node'):
        super(RelatedEntityQuery, self).__init__()

        self.direction = direction
        self.relationship_entity = relationship_entity
        self.relationship_type = relationship_type
        self.start_query_variable = start_query_variable
        self.relationship_query_variable = relationship_query_variable
        self.end_query_variable = end_query_variable
        self._start_entity = None
        self.start_entity = start_entity
        self._end_entity = None
        self.end_entity = end_entity
        self.relationship_prpoerties = relationship_prpoerties or {}
        self.params = params
        self.skip = None
        self.limit = 1 if single_relationship else None

    def reset(self):
        super(RelatedEntityQuery, self).reset()

        self.skip = None
        self.limit = None

        if self.start_entity:
            self.start_entity.query_variable = None

        if self.end_entity:
            self.end_entity.query_variable = None

    def _get_start_entity(self):
        return self._start_entity

    def _set_start_entity(self, entity):
        if entity is not None and not isinstance(entity, Node):
            raise

        if entity:
            entity.query_variable = self.start_query_variable

        self._start_entity = entity

    start_entity = property(_get_start_entity, _set_start_entity)

    def _get_end_entity(self):
        return self._end_entity

    def _set_end_entity(self, entity):
        if entity is not None and not isinstance(entity, Node):
            raise

        if entity:
            entity.query_variable = self.end_query_variable

        self._end_entity = entity

    end_entity = property(_get_end_entity, _set_end_entity)

    def _build_start(self):
        pypher = Pypher()

        if self.start_entity.id:
            qv = self.start_entity.query_variable
            where = __.ID(qv) == self.start_entity.id

            pypher.NODE(qv)
            self.wheres.append(where)
        else:
            pypher.NODE(self.start_query_variable,
                labels=self.start_entity.labels)

        return pypher

    def _build_end(self):
        pypher = Pypher()
        qv = self.end_query_variable
        labels = None

        if self.end_entity:
            qv = self.end_entity.query_variable
            labels = self.end_entity.labels

        pypher.NODE(qv, labels=labels)
        self.returns.append(qv)

        return pypher

    def _build_relationship(self):
        pypher = Pypher()

        if self.relationship_entity:
            self.relationship_entity.query_variable =\
                self.relationship_query_variable

            pypher.relationship(
                self.relationship_entity.query_variable,
                direction=self.direction,
                labels=self.relationship_entity.labels,
                **self.relationship_prpoerties)
        else:
            pypher.relationship(direction=self.direction,
                labels=self.relationship_type)

        return pypher

    def query(self, return_relationship=False, returns=None):
        if not self.start_entity:
            raise RelatedQueryException(('Related objects must have a'
                ' start entity'))

        self.pypher = Pypher()
        pypher = self.pypher

        self.matches.insert(0, self._build_end())
        self.matches.insert(0, self._build_relationship())
        self.matches.insert(0, self._build_start())

        self.pypher.MATCH

        for match in self.matches:
            self.pypher.append(match)

        if self.wheres:
            self.pypher.WHERE.CAND(*self.wheres)

        if self.orders:
            self.pypher.ORDER.BY(*self.orders)

        if return_relationship:
            ret = getattr(__, self.relationship_query_variable)
            self.returns = [ret,]

        returns = returns or self.returns

        self.pypher.RETURN(*returns)

        if self.skip is not None:
            self.pypher.SKIP(self.skip)

        if self.limit is not None:
            self.pypher.LIMIT(self.limit)

        self.reset()

        return str(pypher), pypher.bound_params

    def connect(self, entity, properties=None):
        if not self.start_entity:
            message = ('The relationship {} does not have a start'
                ' entity'.format(self.relationship_entity
                    or self.relationship_type))

            raise RelatedQueryException(('The relationship {} does not '))

        kwargs = {
            'start': self.start_entity,
            'end': entity,
            'properties': properties,
        }

        if self.relationship_entity:
            return self.relationship_entity.__class__(**kwargs)

        kwargs['labels'] = self.relationship_type

        return Relationship(**kwargs)

    def delete(self, entity):
        if isinstance(entity, Relationship):
            if entity.id:
                query = Query(entity)

                return query.delete()
            elif entity.end and entity.end.id:
                self.matches.insert(0, self._build_end())
                self.matches.insert(0, self._build_relationship())
                self.matches.insert(0, self._build_start())

                self.pypher.MATCH

                for match in self.matches:
                    self.pypher.append(match)

                self.pypher.DETACH.DELETE(self.relationship_query_variable)

                return str(self.pypher), self.pypher.bound_params


class QueryException(Exception):
    pass


class RelatedQueryException(QueryException):
    pass


class QueryBuilderException(QueryException):
    pass


class Builder(Pypher):

    def __init__(self, entity, parent=None, params=None, *args, **kwargs):
        if isinstance(entity, Entity):
            VM.set_query_var(entity)
            params = Params(prefix='', key=entity.query_variable)
            self.__entity__ = entity

        super(Builder, self).__init__(parent=parent, params=params, *args,
            **kwargs)

        if isinstance(entity, Relationship):
            self.MATCH.node(self.start).rel(entity.query_variable,
                labels=entity.labels)
            self.node(self.end)
        elif isinstance(entity, Node):
            self.MATCH.node(entity.query_variable, labels=entity.labels)
        else:
            msg = ('The entity {} must be either a Node or '
                'Relationship').format(repr(entity))
            raise QueryBuilderException(msg)

        if entity.id:
            self.WHERE(__.id(self.entity) == entity.id)

    def bind_param(self, value, name=None):
        if not isinstance(value, Param):
            name = VM.get_next(self.entity, name)
            param = Param(name, value)

        return super(Builder, self).bind_param(value=Param, name=name)

    @property
    def start(self):
        return getattr(__, 'start_node')

    @property
    def end(self):
        return getattr(__, 'end_node')

    @property
    def entity(self):
        return getattr(__, self.__entity__.query_variable)


class Helpers(object):

    def get_by_id(self, entity, id_val=None):
        '''This method is used to build a query that will return an entity
        --Node, Relationship-- by its id. It will create a query that looks
        like:

            MATCH ()-[r0:`Labels`]-() WHERE id(r0) = $r0_id_0 RETURN DISTINCT r

        for relationships OR for nodes

            MATCH (n0:`Labels`) WHERE id(n0) = $n0_id_0 RETURN DISTINCT n0
        '''
        entity.id = id_val
        b = Builder(entity)

        b.RETURN.DISTINCT(b.entity)

        return str(b), b.bound_params

    def get_start(self, entity):
        b = Builder(entity)

        b.RETURN(b.start)

        return str(b), b.bound_params

    def get_end(self, entity):
        b = Builder(entity)

        b.RETURN(b.end)

        return str(b), b.bound_params
