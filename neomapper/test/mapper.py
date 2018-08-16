import unittest
import json
import time

from random import random, randint

from neomapper.entity import (Node, StructuredNode, Relationship,
    StructuredRelationship)
from neomapper.property import (String, Integer, TimeStamp)
from neomapper.mapper import (Mapper, EntityMapper, get_mapper)


class TestConnection(object):

    def query(*args, **kwargs):
        class res(object):
            data = []
            result_data = []

        return res()

TC = TestConnection()


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
        mapper = Mapper(TC)
        m = get_mapper(TestLabeldtNode, mapper)
        m2 = mapper.get_mapper(TestLabeldtNode)

        self.assertIsInstance(m, EntityMapper)
        self.assertIsInstance(m, TestLabeldtNodeMapper)
        self.assertIsInstance(m2, EntityMapper)
        self.assertIsInstance(m2, TestLabeldtNodeMapper)
        self.assertEqual(id(m), id(m2))

    def test_can_get_mapper_single_instance_from_entity_instance(self):
        mapper = Mapper(TC)
        tn = TestLabeldtNode()
        m = get_mapper(tn, mapper)
        m2 = mapper.get_mapper(tn)

        self.assertIsInstance(m, EntityMapper)
        self.assertIsInstance(m, TestLabeldtNodeMapper)
        self.assertIsInstance(m2, EntityMapper)
        self.assertIsInstance(m2, TestLabeldtNodeMapper)
        self.assertEqual(id(m), id(m2))

    def test_can_load_correct_mapper_for_entity(self):
        class MyNodeTest(Node):
            pass

        class MyNodeMapperTest(EntityMapper):
            entity = MyNodeTest

        mapper = Mapper(TC)
        my_mapper = mapper.get_mapper(MyNodeTest)

        self.assertIsInstance(my_mapper, MyNodeMapperTest)

    def test_can_load_generic_mapper_for_entity_without_mapper(self):
        node = Node()
        mapper = Mapper(TC)
        my_mapper = mapper.get_mapper(node)

        self.assertEqual(my_mapper.__class__.__name__, 'EntityNodeMapper')

    def test_can_create_generic_node(self):
        mapper = Mapper(TC)
        node = mapper.create()

        self.assertIsInstance(node, Node)

    def test_can_create_custom_node(self):
        class MyNode(Node):
            pass

        mapper = Mapper(TC)
        node = mapper.create(entity=MyNode)

        self.assertIsInstance(node, MyNode)

    def test_can_create_custom_node_with_properties(self):
        class MyNode(Node):
            pass

        mapper = Mapper(TC)
        name = 'name{}'.format(random())
        p = {'name': name}
        node = mapper.create(entity=MyNode, properties=p)
        data = node.data

        self.assertIsInstance(node, MyNode)
        self.assertEqual(1, len(data))
        self.assertEqual(name, data['name'])

    def test_can_create_generic_relationship(self):
        mapper = Mapper(TC)
        relationship = mapper.create(entity_type='relationship')

        self.assertIsInstance(relationship, Relationship)

    def test_can_create_custom_relationship(self):
        class MyRelationship(Relationship):
            pass

        mapper = Mapper(TC)
        rel = mapper.create(entity=MyRelationship)

        self.assertIsInstance(rel, MyRelationship)

    def test_can_create_before_save_event_custom(self):
        mapper = Mapper(TC)

        class MyNode(Node):
            pass

        class MyNodeMapper(EntityMapper):
            entity = MyNode

            def on_before_save(self, entity):
                self.before_save = self.updated

        mn = mapper.create(entity=MyNode)
        my_mapper = mapper.get_mapper(mn)
        my_mapper.before_save = 'BEFOERESAVE'
        my_mapper.updated = 'UDPATED{}'.format(random())
        mapper.save(mn)
        query, params = mapper.prepare()
        mapper.send()

        self.assertEqual(my_mapper.before_save, my_mapper.updated)
        self.assertEqual(1, len(query))
        self.assertIn('CREATE', query[0])

    def test_can_create_after_save_event_custom(self):
        mapper = Mapper(TC)

        class MyNode1(Node):
            pass

        class MyNodeMapper1(EntityMapper):
            entity = MyNode1

            def on_after_save(self, entity, response):
                self.after_save = self.updated

        mn = mapper.create(entity=MyNode1)
        my_mapper = mapper.get_mapper(mn)
        my_mapper.after_save = 'AFTERESAVE'
        my_mapper.updated = 'UDPATED{}'.format(random())
        mapper.save(mn)
        query, params = mapper.prepare()
        mapper.send()

        self.assertEqual(my_mapper.after_save, my_mapper.updated)
        self.assertEqual(1, len(query))
        self.assertIn('CREATE', query[0])

    def test_can_create_before_and_after_save_event_custom(self):
        mapper = Mapper(TC)

        class MyNode2(Node):
            pass

        class MyNodeMapper2(EntityMapper):
            entity = MyNode2

            def on_before_save(self, entity):
                self.before_save = self.updated_before

            def on_after_save(self, entity, response):
                self.after_save = self.updated_after

        mn = mapper.create(entity=MyNode2)
        my_mapper = mapper.get_mapper(mn)
        my_mapper.after_save = 'AFTERESAVE'
        my_mapper.before_save = 'BEFOERESAVE'
        my_mapper.updated_before = 'UDPATED{}'.format(random())
        my_mapper.updated_after = 'UDPATED{}'.format(random())
        mapper.save(mn)
        query, params = mapper.prepare()
        mapper.send()

        self.assertEqual(my_mapper.after_save, my_mapper.updated_after)
        self.assertEqual(my_mapper.before_save, my_mapper.updated_before)
        self.assertEqual(1, len(query))
        self.assertIn('CREATE', query[0])

    def test_can_create_before_and_after_update_events_custom(self):
        mapper = Mapper(TC)

        class MyNode3(Node):
            pass

        class MyNodeMapper3(EntityMapper):
            entity = MyNode3

            def on_before_update(self, entity):
                self.before_update = self.updated_before

            def on_after_update(self, entity, response):
                self.after_update = self.updated_after

        mn = mapper.create(entity=MyNode3, id=999)
        my_mapper = mapper.get_mapper(mn)
        my_mapper.after_update = 'AFTERESAVE'
        my_mapper.before_update = 'BEFOERESAVE'
        my_mapper.updated_before = 'UDPATED{}'.format(random())
        my_mapper.updated_after = 'UDPATED{}'.format(random())
        mapper.save(mn)
        query, params = mapper.prepare()
        mapper.send()

        self.assertEqual(my_mapper.after_update, my_mapper.updated_after)
        self.assertEqual(my_mapper.before_update, my_mapper.updated_before)
        self.assertEqual(1, len(query))
        self.assertIn('MATCH', query[0])

    def test_can_create_before_and_after_delete_events_custom(self):
        mapper = Mapper(TC)

        class MyNode4(Node):
            pass

        class MyNodeMapper4(EntityMapper):
            entity = MyNode4

            def on_before_delete(self, entity):
                self.before_delete = self.deleted_before

            def on_after_delete(self, entity, response):
                self.after_delete = self.deleted_after

        mn = mapper.create(entity=MyNode4, id=999)
        my_mapper = mapper.get_mapper(mn)
        my_mapper.after_delete = 'AFTERDELETE'
        my_mapper.before_delete = 'BEFOEREDELETE'
        my_mapper.deleted_before = 'UDPATED{}'.format(random())
        my_mapper.deleted_after = 'UDPATED{}'.format(random())
        mapper.delete(mn)
        query, params = mapper.prepare()
        mapper.send()

        self.assertEqual(my_mapper.after_delete, my_mapper.deleted_after)
        self.assertEqual(my_mapper.before_delete, my_mapper.deleted_before)
        self.assertEqual(1, len(query))
        self.assertIn('MATCH', query[0])

    def test_can_create_before_and_after_delete_and_save_events_custom(self):
        mapper = Mapper(TC)

        class MyNode4(Node):
            pass

        class MyNodeMapper4(EntityMapper):
            entity = MyNode4

            def on_before_delete(self, entity):
                self.before_delete = self.deleted_before

            def on_after_delete(self, entity, response):
                self.after_delete = self.deleted_after

            def on_before_save(self, entity):
                self.before_save = self.updated_before

            def on_after_save(self, entity, response):
                self.after_save = self.updated_after

        mn = mapper.create(entity=MyNode4)
        my_mapper = mapper.get_mapper(mn)
        my_mapper.after_save = 'AFTERESAVE'
        my_mapper.before_save = 'BEFOERESAVE'
        my_mapper.updated_before = 'UDPATED{}'.format(random())
        my_mapper.updated_after = 'UDPATED{}'.format(random())
        mapper.save(mn)

        mn = mapper.create(entity=MyNode4, id=999)
        my_mapper = mapper.get_mapper(mn)
        my_mapper.after_delete = 'AFTERDELETE'
        my_mapper.before_delete = 'BEFOEREDELETE'
        my_mapper.deleted_before = 'UDPATED{}'.format(random())
        my_mapper.deleted_after = 'UDPATED{}'.format(random())
        mapper.delete(mn)
        query, params = mapper.prepare()
        mapper.send()

        self.assertEqual(my_mapper.after_save, my_mapper.updated_after)
        self.assertEqual(my_mapper.before_save, my_mapper.updated_before)
        self.assertEqual(my_mapper.after_delete, my_mapper.deleted_after)
        self.assertEqual(my_mapper.before_delete, my_mapper.deleted_before)
        self.assertEqual(2, len(query))
        self.assertIn('CREATE', query[0])
        self.assertIn('MATCH', query[1])


class MapperCreateTests(unittest.TestCase):

    def setUp(self):
        self.mapper = Mapper(TC)

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

    def setUp(self):
        self.mapper = Mapper(TC)

    def test_can_udpate_single_node(self):
        id = 999
        name = 'some name'
        n = Node(id=id)
        n['name'] = name

        self.mapper.save(n)
        query, params = self.mapper.prepare()

        self.assertEqual(1, len(query))
        self.assertEqual(2, len(params))
        self.assertIn('SET', query[0])
        self.assertIn(name, params.values())
        self.assertIn(id, params.values())

    def test_can_update_multiple_nodes(self):
        id = 999
        name = 'some name'
        n = Node(id=id)
        n['name'] = name

        id2 = 9992
        name2 = 'some name222'
        n2 = Node(id=id2)
        n2['name'] = name2

        self.mapper.save(n)
        self.mapper.save(n2)
        query, params = self.mapper.prepare()

        self.assertEqual(2, len(query))
        self.assertEqual(4, len(params))
        self.assertIn('SET', query[0])
        self.assertIn('SET', query[1])
        self.assertIn(name, params.values())
        self.assertIn(id, params.values())
        self.assertIn(name2, params.values())
        self.assertIn(id2, params.values())

    def test_can_update_single_relationship(self):
        id = 999
        name = 'some name'
        n = Node(id=id)
        n['name'] = name

        id2 = 9992
        name2 = 'some name222'
        n2 = Node(id=id2)
        n2['name'] = name2
        rid = 9988
        rel = Relationship(start=n, end=n2, id=rid)

        self.mapper.save(rel)
        query, params = self.mapper.prepare()

        self.assertEqual(1, len(query))
        self.assertEqual(5, len(params))
        self.assertIn('SET', query[0])
        self.assertIn(name, params.values())
        self.assertIn(id, params.values())
        self.assertIn(name2, params.values())
        self.assertIn(id2, params.values())
        self.assertIn(rid, params.values())

    def test_can_update_multiple_relationships(self):
        id = 999
        name = 'some name'
        n = Node(id=id)
        n['name'] = name

        id2 = 9992
        name2 = 'some name222'
        n2 = Node(id=id2)
        n2['name'] = name2
        rid = 9988
        rel = Relationship(start=n, end=n2, id=rid)

        id3 = 997
        name3 = 'some name ed'
        n3 = Node(id=id3)
        n3['name'] = name3

        id4 = 99929
        name4 = 'some name222 3'
        n4 = Node(id=id4)
        n4['name'] = name4
        rid2 = 99887
        rel2 = Relationship(start=n3, end=n4, id=rid2)

        self.mapper.save(rel)
        self.mapper.save(rel2)
        query, params = self.mapper.prepare()

        self.assertEqual(2, len(query))
        self.assertEqual(10, len(params))
        self.assertIn('SET', query[0])
        self.assertIn(name, params.values())
        self.assertIn(id, params.values())
        self.assertIn(name2, params.values())
        self.assertIn(id3, params.values())
        self.assertIn(rid, params.values())
        self.assertIn(id4, params.values())
        self.assertIn(name3, params.values())
        self.assertIn(name4, params.values())


class MapperDeleteTests(unittest.TestCase):

    def setUp(self):
        self.mapper = Mapper(TC)

    def test_can_delete_single_node(self):
        _id = 999
        n = Node(id=_id)
        self.mapper.delete(n)
        query, params = self.mapper.prepare()

        self.assertEqual(1, len(query))
        self.assertEqual(1, len(params))
        self.assertTrue('DETACH DELETE' in query[0])

    def test_can_delete_multiple_nodes(self):
        _id = 999
        n = Node(id=_id)
        _id2 = 9998
        n2 = Node(id=_id2)
        self.mapper.delete(n)
        self.mapper.delete(n2)
        query, params = self.mapper.prepare()

        self.assertEqual(2, len(query))
        self.assertEqual(2, len(params))

        for q in query:
            self.assertTrue('DETACH DELETE' in q)

    def test_can_delete_single_relationship(self):
        _id = 999
        n = Node(id=_id)
        _id2 = 999
        n2 = Node(id=_id2)
        _id3 = 8989
        rel = Relationship(start=n, end=n2, id=_id3)
        self.mapper.delete(rel)
        query, params = self.mapper.prepare()

        self.assertEqual(1, len(query))
        self.assertEqual(1, len(params))
        self.assertTrue('DELETE' in query[0])

    def test_can_delete_multiple_relationships(self):
        _id = 999
        n = Node(id=_id)
        _id2 = 999
        n2 = Node(id=_id2)
        _id3 = 8989
        rel = Relationship(start=n, end=n2, id=_id3)

        _iid = 134
        nn = Node(id=_iid)
        _id22 = 323
        nn2 = Node(id=_id22)
        _id4 = 9991
        rel2 = Relationship(start=nn, end=nn2, id=_id4)
        self.mapper.delete(rel).delete(rel2)
        query, params = self.mapper.prepare()

        self.assertEqual(2, len(query))
        self.assertEqual(2, len(params))

        for q in query:
            self.assertTrue('DELETE' in q)

# TODO move to integration testing
# class MapperBuilderTests(unittest.TestCase):
#
#     def setUp(self):
#         self.mapper = Mapper(TC)
#
#     def test_can_get_by_id(self):
#         import pudb; pu.db
#         id_val = randint(1, 9999)
#         query, params = self.mapper.get_by_id(id_val=id_val)
#         import pudb; pu.db


if __name__ == '__main__':
    unittest.main()
