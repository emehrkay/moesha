import copy


class _Entity(object):

    def __init__(self, pk=None, properties=None):
        properties = properties or {}
        self.pk = pk
        self.hydrate(**properties)
        self.changes = {}

    def hydrate(self, **kwargs):
        self.data = copy.copy(kwargs)
        self.initial = copy.copy(kwargs)

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
