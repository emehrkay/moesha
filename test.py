from moesha.entity import Node, Relationship
from moesha.mapper import Mapper, get_mapper, EntityMapper
from moesha.property import *
from moesha.connection import Connection
from moesha.query import Builder



c = Connection(host='localhost', port=7687, username='neo4j', password='gotskills')
m = Mapper(connection=c)


class BM(EntityMapper):
    __PROPERTIES__ = {
        'ROOT_TIME': TimeStamp(),
    }


class BaseMapper(BM):
    __PROPERTIES__ = {
        'created_date': TimeStamp(),
    }


class BaseMapper2(EntityMapper):
    __PROPERTIES__ = {
        'location': String(default='Baltimore'),
    }


class Person(Node):
    pass


class PersonMapper(BaseMapper, BaseMapper2):
    entity = Person
    __LABELS__ = ['Person', 'Animal']
    __PROPERTIES__ = {
        'name': String(),
        'age': Integer(default=99999),
        'ROOT_TIME': Integer(default=7777777),
    }

class User(Node):
    pass


class UserMapper(BaseMapper, BaseMapper2):
    entity = User



import pudb; pu.db
um = m.get_mapper(User)
p = um.get_by_id(192)
b = Builder(p)
b.SET(b.entity.__location__ == 'philly', b.entity.__locationx__ == 'phillxy')
ax = str(b)
print(ax)
# pm = m.get_mapper(Person)
# properties={'name': 'MARKKKKKKK', 'sexiii': 'male', 't': 999, 'oo oo 99': 88}
# e = m.create(properties=properties, label=['Person', 'Animal'])
# e['name'] = 'oooo'
# e2 = pm.create(properties=properties)
# e2['age'] = 00000
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