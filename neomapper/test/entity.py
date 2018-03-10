import unittest
import json
import time

from random import random

from neomapper.entity import (Node, StructuredNode, Relationship,
    StructuredRelationship)
from neomapper.property import (String, Integer, TimeStamp)


class TestNode(Node):
    pass


class TesLabeldtNode(Node):
    _LABELS = 'TEST_LABEL_NODE'


class Test_Unlabeled_Node(Node):
    pass


class EntityTests(unittest.TestCase):

    def test_can_create_entity(self):
        n = TestNode()

        self.assertIsInstance(n, Node)

    def test_can_create_entities_with_labels(self):
        t = TestNode()
        tl = TesLabeldtNode()
        tu = Test_Unlabeled_Node()
        t_ls = ['TestNode']
        tl_ls = ['TEST_LABEL_NODE']
        tu_ls = ['Test', 'Unlabeled', 'Node']

        def test_labels(labels, node):
            self.assertEqual(len(labels), len(node.label))

            for l in labels:
                self.assertIn(l, node.label)

        test_labels(t_ls, t)
        test_labels(tl_ls, tl)
        test_labels(tu_ls, tu)

    def test_can_inherit_properties_label_and_undefined(self):
        _label = 'SOME LABEL'

        class One(Node):
            _ALLOW_UNDEFINED = True
            name = String()

        class Two(One):
            _LABELS = [_label]
            age = Integer()

        class TwoTwo(One):
            location = String()

        class Three(Two, TwoTwo):
            _ALLOW_UNDEFINED = False
            sex = String()

        t = Three()
        exp = ['name', 'age', 'sex', 'location']

        for e in exp:
            self.assertIn(e, t.data)

        self.assertIn(_label, t.label)
        self.assertFalse(t.properties.allow_undefined)

    def test_can_create_entity_with_data(self):
        _label = 'SOME LABEL'

        class One(Node):
            _ALLOW_UNDEFINED = True
            name = String()

        class Two(One):
            _LABELS = [_label]
            age = Integer()

        class TwoTwo(One):
            location = String()

        class Three(Two, TwoTwo):
            _ALLOW_UNDEFINED = False
            sex = String()

        props = {'name': 'mark', 'age': 999, 'location': 'earth'}
        t = Three(properties=props)

        self.assertEqual(props['name'], t['name'])
        self.assertEqual(props['age'], t['age'])
        self.assertEqual(props['location'], t['location'])
        self.assertEqual('', t['sex'])

    def test_can_add_undefined_property_to_entity(self):

        class T(Node):
            pass

        name = str(random())
        t = T()
        t['name'] = name

        self.assertEqual(name, t['name'])
        self.assertEqual(1, len(t.data))

    def test_cannot_add_undefined_property_to_entity(self):

        class T(StructuredNode):
            pass

        name = str(random())
        t = T()
        t['name'] = name

        self.assertEqual(None, t['name'])
        self.assertEqual(0, len(t.data))

    def test_can_change_datatype_for_entity(self):
        t = TestNode()
        t.data_type = 'graph'

        self.assertEqual('graph', t.data_type)

    def test_can_delete_dynamically_added_field(self):
        t = TestNode()
        f = 'name'
        t[f] = 'some name'

        del(t[f])

        self.assertNotIn(f, t.data)
        self.assertEqual(0, len(t.data))

    def test_can_delete_defined_field(self):
        class X(StructuredNode):
            name = String()

        t = X()
        f = 'name'
        t[f] = 'some name'

        del(t[f])

        self.assertNotIn(f, t.data)
        self.assertEqual(0, len(t.data))

    def test_can_hydrate_with_new_data(self):
        p = {'name': 'mark'}
        t = TestNode(properties=p)
        np = {'name': 'mark2', 'age': 999}
        t.hydrate(**np)

        self.assertEqual(2, len(t.data))

        for n, v in np.items():
            self.assertIn(n, t.data)
            self.assertEqual(v, t[n])

    def test_can_force_hyrdate_defined_fields(self):
        class X(StructuredNode):
            time = TimeStamp()

        t = X()
        p = {'time': 99999}
        t.force_hydrate(**p)

        self.assertEqual(1, len(t.data))
        self.assertEqual(p['time'], t['time'])

    def test_cannot_hyrdate_defined_fields(self):
        class X(StructuredNode):
            time = TimeStamp()

        t = X()
        p = {'time': 99999}
        t.hydrate(**p)

        self.assertEqual(1, len(t.data))
        self.assertNotEqual(p['time'], t['time'])


if __name__ == '__main__':
    unittest.main()
