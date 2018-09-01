from moesha2.entity import Node, Relationship
from moesha2.mapper import Mapper, get_mapper, EntityMapper
from moesha2.property import *
from moesha2.connection import Connection



c = Connection(host='localhost', port=7687, username='neo4j', password='test')
m = Mapper(connection=c)



class Person(Node):
    pass


class PersonMapper(EntityMapper):
    entity = Person
    __PROPERTIES__ = {
        'name': String(),
        'age': Integer(default=99999),
    }


import pudb; pu.db
properties={'name': 'MARKKKKKKK', 'sexiii': 'male', 't': 999, 'oo oo 99': 88}
e = m.create(properties=properties, label=['Person'])
e['name'] = 'oooo'
# rs = m.query(query='MATCH ()-[r]->() return r')
# for r in rs:
#     print(r.id, r.data, '==', r.start, '--', r.end)
# # for r in rs:
# #     print(r.id, r.data, '==', r.start, '--', r.end)
# # pm(e).knows()
# # m.save(e)
# # m.send()
# # print(']]]]]]]]]', e.id)
# import pudb; pu.db
# rm = m.get_mapper(r)
# entity = rm.get_by_id(117)
# x = 1