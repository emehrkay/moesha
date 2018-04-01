import unittest
import json
import time

from random import random

from neomapper.entity import (Node, StructuredNode, Relationship,
    StructuredRelationship)
from neomapper.property import (String, Integer, TimeStamp)
from neomapper.mapper import (Mapper, EntityMapper, get_mapper)


class TestNode(Node):
    pass


class TestLabeldtNode(Node):
    _LABELS = 'TEST_LABEL_NODE'


class TestRelationship(Relationship):
    _LABELS = 'TEST_RELATIONSHIP'


class TestLabeldtNodeMapper(EntityMapper):
    entity = TestLabeldtNode


class MapperTests(unittest.TestCase):

    def test_can_create_instance(self):
        m = TestLabeldtNodeMapper()

        self.assertIsInstance(m, EntityMapper)

    def test_can_get_mapper_single_instance_from_entity_class(self):
        mapper = Mapper(None)
        m = get_mapper(TestLabeldtNode, mapper)
        m2 = mapper.get_mapper(TestLabeldtNode)

        self.assertIsInstance(m, EntityMapper)
        self.assertIsInstance(m, TestLabeldtNodeMapper)
        self.assertIsInstance(m2, EntityMapper)
        self.assertIsInstance(m2, TestLabeldtNodeMapper)
        self.assertEqual(id(m), id(m2))

    def test_can_get_mapper_single_instance_from_entity_instance(self):
        mapper = Mapper(None)
        tn = TestLabeldtNode()
        m = get_mapper(tn, mapper)
        m2 = mapper.get_mapper(tn)

        self.assertIsInstance(m, EntityMapper)
        self.assertIsInstance(m, TestLabeldtNodeMapper)
        self.assertIsInstance(m2, EntityMapper)
        self.assertIsInstance(m2, TestLabeldtNodeMapper)
        self.assertEqual(id(m), id(m2))


class MapperCreateTests(unittest.TestCase):

    def setUp(self):
        self.mapper = Mapper(None)

        return self

    def tearDown(self):
        self.mapper.reset()

    def test_mapper_can_create_single_node(self):
        name = 'mark {}'.format(random())
        p = {'name': name}
        n = Node(properties=p)

        self.mapper.save(n)
        queries, params = self.mapper.prepare()

        self.assertEqual(1, len(params))
        self.assertEqual(1, len(queries))
        self.assertTrue(queries[0].startswith('CREATE'))

    def test_mapper_can_create_multiple_nodes(self):
        name = 'mark {}'.format(random())
        p = {'name': name}
        n = Node(properties=p)
        n2 = Node(properties=p)
        n3 = Node(properties=p)

        self.mapper.save(n)
        self.mapper.save(n2)
        self.mapper.save(n3)

        queries, params = self.mapper.prepare()

        self.assertEqual(1, len(params))
        self.assertEqual(3, len(queries))

        for query in queries:
            self.assertTrue(query.startswith('CREATE'))

    def test_can_create_single_relationship(self):
        p = {'name': 'somename'}
        start = Node(properties=p)
        end = Node(properties=p)
        rel = Relationship(start=start, end=end)

        self.mapper.save(rel)
        query, params = self.mapper.prepare()

        self.assertEqual(1, len(query))
        self.assertEqual(1, len(params))
        self.assertTrue(query[0].startswith('CREATE'))
        self.assertTrue('RETURN' in query[0])


class MapperUpdateTests(unittest.TestCase):
    pass


class MapperDeleteTests(unittest.TestCase):
    pass


class MapperCombinedTests(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
