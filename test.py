from neomapper.entity import Node, ENTITY_MAP
from neomapper.mapper import Mapper, Relationship, get_mapper, EntityMapper, RelatedEntity
from neomapper.property import *


m = Mapper(connection=None)


class Location(Node):
    city = String(default='Baltimore')


class HasLocation(Relationship):
    pass


class Person(Node):
    _ALLOW_UNDEFINED = True
    _LABELS = ['Person', 'Animal', 'Human']
    name = String(default='some name')
    t = TimeStamp()


class Animal(Node):
    pass


class PersonMapper(EntityMapper):
    entity = Person
    location = RelatedEntity(relationship_entity=HasLocation)


pm = get_mapper(Person, m)
pm2 = get_mapper(Person, m)

print(id(pm), id(pm2))


properties={'name': 'MARKKKKKKK', 'sexiii': 'male', 't': 999, 'oo oo 99': 88}

e = m.create(id=10, properties=properties, label=['Person', 'Animal', 'Human'])

pm(e).location()
x = 1