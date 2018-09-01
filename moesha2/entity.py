import copy


class _Entity(object):

    def __init__(self, id=None, properties=None):
        properties = properties or {}
        self.id = id
        self._data = {}
        self._initial = {}
        self._changes = {}

        self.hydrate(properties=properties, reset=True)

    @property
    def data(self):
        return self._data

    @property
    def changes(self):
        return self._changes

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


class Node(_Entity):
    pass


class Relationship(_Entity):

    def __init__(self, id=None, start=None, end=None, properties=None):
        super(Relationship, self).__init__(id=id, properties=properties)
        self.start = start
        self.end = end


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
