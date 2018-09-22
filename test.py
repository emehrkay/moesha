from moesha.entity import Node, Relationship
from moesha.mapper import Mapper, get_mapper, EntityMapper
from moesha.property import *
from moesha.connection import Connection
from moesha.query import Builder



c = Connection(host='localhost', port=7687, username='neo4j',
    password='gotskills_testing')
m = Mapper(connection=c)


class Comment(Node):
    pass


class CommentMappeer(EntityMapper):
    entity = Comment
    __PROPERTIES__ = {
        'text': String()
    }


class HasComment(Relationship):
    pass


class Person(Node):
    pass


class PersonMapper(EntityMapper):
    entity = Person
    __LABELS__ = ['Person', 'Animal']
    __PROPERTIES__ = {
        'name': String(),
        'age': Integer(default=99999),
        'ROOT_TIME': Integer(default=7777777),
    }
    __RELATIONSHIPS__ = {
        'Comments': RelatedEntity(relationship_entity=HasComment),
    }


pm = m.get_mapper(Person)
person = m.create(entity=Person, properties={'name': 'mark'})

for i in range(20):
    comment = m.create(entity=Comment, properties={'text': 'some comment {}'.format(i)})
    m.save(person, comment).send()
    # import pudb; pu.db
    hc = pm(person)['Comments'].add(comment)
    m.send()
# import pudb; pu.db
cs = pm(person)['Comments']()
