import copy

from .util import entity_to_labels


class Entity(object):

    def __init__(self, id=None, labels=None, properties=None):
        properties = properties or {}
        self.id = id
        self._data = {}
        self._initial = {}
        self._changes = {}
        self.labels = labels or []

        self.hydrate(properties=properties, reset=True)

    @property
    def data(self):
        return self._data

    @property
    def changes(self):
        return self._changes

    def _get_labels(self):
        self._labels.sort()

        return self._labels

    def _set_labels(self, labels):
        if not labels:
            labels = entity_to_labels(self).split(':')

        if not isinstance(labels, (list, set, tuple)):
            labels = [labels,]

        labels.sort()

        self._labels = labels

        return self

    labels = property(_get_labels, _set_labels)

    def hydrate(self, properties=None, reset=False):
        properties = properties or {}

        if reset:
            self._data = copy.copy(properties)
            self._initial = copy.copy(properties)
        else:
            for k, v in properties.items():
                self[k] = v

        return self

    def __getitem__(self, name):
        return self._data.get(name, None)

    def __setitem__(self, name, value):
        if name in self._initial:
            if value != self._initial[name]:
                self._changes[name] = {
                    'from': self._initial[name],
                    'to': value,
                }
            else:
                try:
                    del self._changes[name]
                except:
                    pass

        self._data[name] = value

        return self


class Node(Entity):
    pass


class Relationship(Entity):

    def __init__(self, id=None, start=None, end=None, properties=None,
                 labels=None):
        super(Relationship, self).__init__(id=id, properties=properties,
            labels=labels)
        self.start = start
        self.end = end

    def _get_labels(self):
        labels = super(Relationship, self).labels

        return labels[0]

    def _set_labels(self, labels):
        return super(Relationship, self)._set_labels(labels=labels)

    labels = property(_get_labels, _set_labels)

    @property
    def type(self):
        return self.labels


class Collection(object):

    def __init__(self, entities=None):
        if entities and not isinstance(entities, (list, set, tuple)):
            entities = [entities,]
        elif isinstance(entities, Collection):
            entities = entities.entities

        self.entities = entities or []
        self.index = 0

    def __getitem__(self, field):
        return [entity[field] for entity in self]

    def __setitem__(self, field, value):
        for entity in self:
            entity[field] = value

        return self

    def __delitem__(self, field):
        for entity in self:
            del(entity[field])

        return self

    def __iter__(self):
        return self
    
    def __iter__(self):
        return self

    def __next__(self):
        entity = self[self.index]
        self.index += 1

        return entity
