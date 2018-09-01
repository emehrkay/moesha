import copy


class _Entity(object):

    def __init__(self, id=None, properties=None):
        properties = properties or {}
        self.id = id
        self.data = {}
        self.initial = {}
        self.changes = {}

        self.hydrate(properties=properties, reset=True)

    def hydrate(self, properties=None, reset=False):
        properties = properties or {}

        if reset:
            self.data = copy.copy(properties)
            self.initial = copy.copy(properties)
        else:
            self.data.update(properties)

        return self

    def __getitem__(self, name):
        return self.data.get(name, None)

    def __setitem__(self, name, value):
        if name in self.initial:
            if value != self.initial[name]:
                self.changes[name] = {
                    'from': self.initial[name],
                    'to': value,
                }
            else:
                try:
                    del self.changes[name]
                except:
                    pass

        self.data[name] = value

        return self


class Node(_Entity):
    pass


class Relationship(_Entity):

    def __init__(self, pk=None, start=None, end=None, properties=None):
        super(Relationship, self).__init__(pk=pk, properties=properties)
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
