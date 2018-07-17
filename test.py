from neomapper.entity import Node, ENTITY_MAP
from neomapper.mapper import Mapper, Relationship, get_mapper, EntityMapper, RelatedEntity
from neomapper.property import *
from neomapper.connection import Connection



c = Connection(host='localhost', port=7687, username='neo4j', password='test')
m = Mapper(connection=c)


class Location(Node):
    city = String(default='Baltimore')


class Knows(Relationship):
    _LABELS = ['knows']


class Person(Node):
    _ALLOW_UNDEFINED = True
    _LABELS = ['Person', 'Animal', 'Human']
    name = String(default='some name')
    t = TimeStamp()


class Animal(Node):
    pass


class PersonMapper(EntityMapper):
    entity = Person
    knows = RelatedEntity(relationship_entity=Knows)


pm = get_mapper(Person, m)
pm2 = get_mapper(Person, m)

print(id(pm), id(pm2))


properties={'name': 'MARKKKKKKK', 'sexiii': 'male', 't': 999, 'oo oo 99': 88}
e = m.create(properties=properties, label=['Person', 'Animal', 'Human'])
rs = m.query(query='MATCH ()-[r]->() return r')
for r in rs:
    print(r.id, r.data, '==', r.start, '--', r.end)
# for r in rs:
#     print(r.id, r.data, '==', r.start, '--', r.end)
# pm(e).knows()
# m.save(e)
# m.send()
# print(']]]]]]]]]', e.id)
import pudb; pu.db
rm = m.get_mapper(r)
entity = rm.get_by_id(117)
x = 1