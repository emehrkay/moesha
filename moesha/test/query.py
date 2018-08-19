import unittest

from random import random, randint

from moesha.entity import (Node, Relationship)
from moesha.query import (Query, RelationshipQuery, QueryException,
    RelatedQueryException)
from moesha.mapper import (Mapper)


def get_dict_key(dict, value):
    for k, v in dict.items():
        if v == value:
            return k

    return None


class NodeQueryTests(unittest.TestCase):

    def test_can_build_single_node_create_query(self):
        name = 'mark {}'.format(random())
        n = Node(properties={'name': name})
        q = Query(n)
        query, params = q.save()
        exp = 'CREATE ({var}:`{label}` {{`name`: ${val}}}) RETURN {var}'.format(
            var=n.query_variable, val=get_dict_key(params, name),
            label='Node')

        self.assertEqual(exp, query)
        self.assertEqual(1, len(params))

    def test_can_build_mutiple_node_create_query(self):
        name = 'mark {}'.format(random())
        n = Node(properties={'name': name})
        name2 = 'mark {}'.format(random())
        n2 = Node(properties={'name': name2})
        q = Query([n, n2])
        query, params = q.save()
        exp = ('CREATE ({var}:`{label1}` {{`name`: ${val}}}), '
            '({var2}:`{label2}` {{`name`: ${val2}}}) '
            'RETURN {var}, {var2}').format(
            var=n.query_variable, val=get_dict_key(params, name),
            var2=n2.query_variable, val2=get_dict_key(params, name2),
            label1='Node', label2='Node')

        self.assertEqual(exp, query)
        self.assertEqual(2, len(params))

    def test_can_build_single_node_update_query(self):
        name = 'mark {}'.format(random())
        _id = 999
        n = Node(id=_id, properties={'name': name})
        q = Query(n)
        query, params = q.save()
        exp = "MATCH ({var}) SET {var}.`name` = ${val} WHERE id({var}) = ${id} RETURN {var}".format(
            var=n.query_variable, val=get_dict_key(params, name),
            id=get_dict_key(params, _id))

        self.assertEqual(exp, query)
        self.assertEqual(2, len(params))

    def test_can_build_multiple_node_update_query(self):
        name = 'mark {}'.format(random())
        _id = 999
        n = Node(id=_id, properties={'name': name})
        name2 = 'kram {}'.format(random())
        _id2 = 888
        n2 = Node(id=_id2, properties={'name': name2})
        q = Query([n, n2])
        query, params = q.save()
        exp = ("MATCH ({var}), ({var2}) "
            "SET {var}.`name` = ${val}, {var2}.`name` = ${val2} "
            "WHERE id({var}) = ${id} AND id({var2}) = ${id2} "
            "RETURN {var}, {var2}").format(
            var=n.query_variable, val=get_dict_key(params, name),
            id=get_dict_key(params, _id), var2=n2.query_variable,
            val2=get_dict_key(params, name2), id2=get_dict_key(params, _id2))

        self.assertEqual(exp, query)
        self.assertEqual(4, len(params))

    def test_can_delete_single_existing_node(self):
        _id = 999
        n = Node(id=_id)
        q = Query(n)
        query, params = q.delete()
        exp = "MATCH ({var}) WHERE id({var}) = ${id} DELETE {var}".format(
            var=n.query_variable, id=get_dict_key(params, _id))

        self.assertEqual(exp, query)
        self.assertEqual(1, len(params))

    def test_can_detach_delete_single_existing_node(self):
        _id = 999
        n = Node(id=_id)
        q = Query(n)
        query, params = q.delete(detach=True)
        exp = "MATCH ({var}) WHERE id({var}) = ${id} DETACH DELETE {var}".format(
            var=n.query_variable, id=get_dict_key(params, _id))

        self.assertEqual(exp, query)
        self.assertEqual(1, len(params))

    def test_can_delete_multiple_existing_nodes(self):
        _id = 999
        n = Node(id=_id)
        _id2 = 777
        n2 = Node(id=_id2)
        q = Query([n, n2])
        query, params = q.delete()
        exp = ("MATCH ({var}) WHERE id({var}) = ${id}"
            " MATCH ({var2}) WHERE id({var2}) = ${id2}"
            " DELETE {var}, {var2}".format(
            var=n.query_variable, id=get_dict_key(params, _id),
            var2=n2.query_variable, id2=get_dict_key(params, _id2)))

        self.assertEqual(exp, query)
        self.assertEqual(2, len(params))

    def test_can_detach_delete_multiple_existing_nodes(self):
        _id = 999
        n = Node(id=_id)
        _id2 = 777
        n2 = Node(id=_id2)
        q = Query([n, n2])
        query, params = q.delete(detach=True)
        exp = ("MATCH ({var}) WHERE id({var}) = ${id}"
            " MATCH ({var2}) WHERE id({var2}) = ${id2}"
            " DETACH DELETE {var}, {var2}".format(
            var=n.query_variable, id=get_dict_key(params, _id),
            var2=n2.query_variable, id2=get_dict_key(params, _id2)))

        self.assertEqual(exp, query)
        self.assertEqual(2, len(params))

    def test_can_delete_multiple_existing_nodes_with_id(self):
        _id = 999
        n = Node(id=_id)
        _id2 = 777
        n2 = Node(id=_id2)
        n3 = Node()
        q = Query([n, n2])
        query, params = q.delete(detach=True)
        exp = ("MATCH ({var}) WHERE id({var}) = ${id}"
            " MATCH ({var2}) WHERE id({var2}) = ${id2}"
            " DETACH DELETE {var}, {var2}".format(
            var=n.query_variable, id=get_dict_key(params, _id),
            var2=n2.query_variable, id2=get_dict_key(params, _id2)))

        self.assertEqual(exp, query)
        self.assertEqual(2, len(params))


class RelationshipQueryTests(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

    def test_can_build_single_create_relationship_with_existing_nodes_create_query(self):
        sid = 99
        n = 'mark {}'.format(random())
        start = Node(id=sid, properties={'name': n})
        eid = 88
        n2 = 'kram {}'.format(random())
        end = Node(id=eid, properties={'name': n2})
        since = 'yeserday'
        rel = Relationship(start=start, end=end, properties={'since': since})
        q = Query(rel)
        query, params = q.save()
        label = rel.label
        exp = ("CREATE ({var})-[{var3}:`{label}` {{`since`: ${since}}}]->({var2})"
            " SET {var}.`name` = ${val1}, {var2}.`name` = ${val2}"
            " WHERE id({var}) = ${id} AND id({var2}) = ${id2}"
            " RETURN {var}, {var2}, {var3}").format(var=start.query_variable,
                var2=end.query_variable, var3=rel.query_variable, label=label,
                id=get_dict_key(params, sid), id2=get_dict_key(params, eid),
                val1=get_dict_key(params, n), val2=get_dict_key(params, n2),
                since=get_dict_key(params, since))

        self.assertEqual(exp, query)
        self.assertEqual(5, len(params))

    def test_can_build_single_create_relationship_with_two_new_nodes_create_query(self):
        n = 'mark {}'.format(random())
        start = Node(properties={'name': n})
        n2 = 'kram {}'.format(random())
        end = Node(properties={'name': n2})
        since = 'yeserday'
        rel = Relationship(start=start, end=end, properties={'since': since})
        q = Query(rel)
        query, params = q.save()
        label = rel.label
        exp = ("CREATE ({var}:`{vlabel}` {{`name`: ${name}}})-[{rel}:`{label}` {{`since`: ${since}}}]->({var2}:`{vlabel}` {{`name`: ${name2}}})"
            " RETURN {var}, {var2}, {rel}".format(var=start.query_variable,
                rel=rel.query_variable, label='Relationship',
                since=get_dict_key(params, since), var2=end.query_variable,
                name=get_dict_key(params, n), name2=get_dict_key(params, n2),
                vlabel='Node'))

        self.assertEqual(exp, query)
        self.assertEqual(3, len(params))

    def test_can_build_single_create_relationship_with_one_existing_one_new_node_create_query(self):
        sid = 99
        n = 'mark {}'.format(random())
        start = Node(id=sid, properties={'name': n})
        n2 = 'kram {}'.format(random())
        end = Node(properties={'name': n2})
        since = 'yeserday'
        rel = Relationship(start=start, end=end, properties={'since': since})
        q = Query(rel)
        query, params = q.save()
        label = rel.label
        exp = ("CREATE ({var})-[{rel}:`{label}` {{`since`: ${since}}}]->({var2}:`Node` {{`name`: ${name2}}})"
            " SET {var}.`name` = ${name}"
            " WHERE id({var}) = ${id}"
            " RETURN {var}, {var2}, {rel}".format(var=start.query_variable,
                id=get_dict_key(params, sid),
                rel=rel.query_variable, label='Relationship',
                since=get_dict_key(params, since), var2=end.query_variable,
                name=get_dict_key(params, n), name2=get_dict_key(params, n2)))

        self.assertEqual(exp, query)
        self.assertEqual(4, len(params))

    def test_can_build_single_create_multiple_relationship_with_the_same_existing_nodes_create_query(self):
        sid = 99
        n = 'mark {}'.format(random())
        start = Node(id=sid, properties={'name': n})
        eid = 88
        n2 = 'kram {}'.format(random())
        end = Node(id=eid, properties={'name': n2})
        since = 'yeserday'
        label2 = 'knows_two'
        rel = Relationship(start=start, end=end, properties={'since': since})
        rel2 = Relationship(start=start, end=end, labels=label2, properties={'since': since})
        q = Query([rel, rel2])
        query, params = q.save()

        label = rel.label
        exp = ("CREATE ({var})-[{var3}:`{label}` {{`since`: ${since}}}]->({var2}),"
            " ({var})-[{var4}:`{label2}` {{`since`: ${since}}}]->({var2})"
            " SET {var}.`name` = ${val1}, {var2}.`name` = ${val2}"
            " WHERE id({var}) = ${id} AND id({var2}) = ${id2}"
            " RETURN {var}, {var2}, {var3}, {var4}").format(var=start.query_variable,
                var2=end.query_variable, var3=rel.query_variable, label=label,
                id=get_dict_key(params, sid), id2=get_dict_key(params, eid),
                val1=get_dict_key(params, n), val2=get_dict_key(params, n2),
                since=get_dict_key(params, since), var4=rel2.query_variable,
                label2=label2)

        self.assertEqual(exp, query)
        self.assertEqual(5, len(params))

    def test_can_build_single_create_multiple_relationship_with_different_existing_nodes_create_query(self):
        sid = 99
        name = 'mark {}'.format(random())
        start = Node(id=sid, properties={'name': name})
        eid = 88
        name2 = 'kram {}'.format(random())
        end = Node(id=eid, properties={'name': name2})
        sid2 = 999
        name3 = 'mark {}'.format(random())
        start2 = Node(id=sid2, properties={'name': name3})
        eid2 = 888
        name4 = 'kram {}'.format(random())
        end2 = Node(id=eid2, properties={'name': name4})
        since = 'yeserday'
        since2 = 'some time ago'
        label2 = 'knows_two'
        rel = Relationship(start=start, end=end, properties={'since': since})
        rel2 = Relationship(start=start2, end=end2, labels=label2,
            properties={'since': since2})
        q = Query([rel, rel2])
        query, params = q.save()

        label = rel.label[0]
        exp = ("CREATE ({var})-[{rel}:`Relationship` {{`since`: ${since}}}]->({var2}),"
            " ({var3})-[{rel2}:`{label}` {{`since`: ${since2}}}]->({var4})"
            " SET {var}.`name` = ${name}, {var2}.`name` = ${name2}, {var3}.`name` = ${name3}, {var4}.`name` = ${name4}"
            " WHERE id({var}) = ${id} AND id({var2}) = ${id2} AND id({var3}) = ${id3} AND id({var4}) = ${id4}"
            " RETURN {var}, {var2}, {rel}, {var3}, {var4}, {rel2}").format(
                var=start.query_variable, id=get_dict_key(params, sid),
                var2=end.query_variable, id2=get_dict_key(params, eid),
                var3=start2.query_variable, id3=get_dict_key(params, sid2),
                var4=end2.query_variable, id4=get_dict_key(params, eid2),
                rel=rel.query_variable, since=get_dict_key(params, since),
                rel2=rel2.query_variable, since2=get_dict_key(params, since2),
                label=label2, name=get_dict_key(params, name),
                name2=get_dict_key(params, name2), name3=get_dict_key(params, name3),
                name4=get_dict_key(params, name4))

        self.assertEqual(exp, query)
        self.assertEqual(10, len(params))

    def test_can_build_single_create_multiple_relationship_with_different_existing_and_new_nodes_create_query(self):
        name = 'mark {}'.format(random())
        start = Node(properties={'name': name})
        eid = 88
        name2 = 'kram {}'.format(random())
        end = Node(id=eid, properties={'name': name2})
        name3 = 'mark {}'.format(random())
        start2 = Node(properties={'name': name3})
        eid2 = 888
        name4 = 'kram {}'.format(random())
        end2 = Node(id=eid2, properties={'name': name4})
        since = 'yeserday'
        since2 = 'some time ago'
        label2 = 'knows_two'
        rel = Relationship(start=start, end=end, properties={'since': since})
        rel2 = Relationship(start=start2, end=end2, labels=label2,
            properties={'since': since2})
        q = Query([rel, rel2])
        query, params = q.save()

        label = rel.label
        exp = ("CREATE ({var}:`Node` {{`name`: ${name}}})-[{rel}:`Relationship` {{`since`: ${since}}}]->({var2}),"
            " ({var3}:`Node` {{`name`: ${name3}}})-[{rel2}:`{label}` {{`since`: ${since2}}}]->({var4})"
            " SET {var2}.`name` = ${name2}, {var4}.`name` = ${name4}"
            " WHERE id({var2}) = ${id2} AND id({var4}) = ${id4}"
            " RETURN {var}, {var2}, {rel}, {var3}, {var4}, {rel2}").format(
                var=start.query_variable,
                var2=end.query_variable, id2=get_dict_key(params, eid),
                var3=start2.query_variable,
                var4=end2.query_variable, id4=get_dict_key(params, eid2),
                rel=rel.query_variable, since=get_dict_key(params, since),
                rel2=rel2.query_variable, since2=get_dict_key(params, since2),
                label=label2, name=get_dict_key(params, name),
                name2=get_dict_key(params, name2), name3=get_dict_key(params, name3),
                name4=get_dict_key(params, name4))

        self.assertEqual(exp, query)
        self.assertEqual(8, len(params))

    def test_can_build_single_update_query(self):
        sid = 99
        name = 'mark {}'.format(random())
        start = Node(id=sid, properties={'name': name})
        eid = 88
        name2 = 'kram {}'.format(random())
        end = Node(id=eid, properties={'name': name2})
        rid = 447788
        since = 'since {}'.format(random())
        rel = Relationship(id=rid, start=start, end=end, properties={'since': since})
        q = Query(rel)
        query, params = q.save()

        exp = ("MATCH ({start})-[{rel}:`{label}`]->({end})"
            " SET {start}.`name` = ${name}, {end}.`name` = ${name2}, {rel}.`since` = ${since}"
            " WHERE id({start}) = ${sid} AND id({end}) = ${eid} AND id({rel}) = ${rid}"
            " RETURN {start}, {end}, {rel}".format(start=start.query_variable,
                sid=get_dict_key(params, sid), end=end.query_variable,
                eid=get_dict_key(params, eid), rel=rel.query_variable,
                label='Relationship', name=get_dict_key(params, name),
                name2=get_dict_key(params, name2), rid=get_dict_key(params, rid),
                since=get_dict_key(params, since)))

        self.assertEqual(exp, query)
        self.assertEqual(6, len(params))

    def test_can_build_mixed_update_and_insert_query(self):
        sid = 99
        name = 'mark {}'.format(random())
        start = Node(id=sid, properties={'name': name})
        eid = 88
        name2 = 'kram {}'.format(random())
        end = Node(id=eid, properties={'name': name2})
        rid = 447788
        since = 'since {}'.format(random())
        rel = Relationship(id=rid, start=start, end=end, properties={'since': since})
        sid2 = 887
        name3 = 'name {}'.format(random())
        start2 = Node(id=sid2, properties={'name': name3})
        name4 = 'name {}'.format(random())
        end2 = Node(properties={'name': name4})
        rel2 = Relationship(start=start2, end=end2, properties={'since': since})
        q = Query([rel, rel2])
        query, params = q.save()

        exp = ("MATCH ({start})-[{rel}:`{label}`]->({end})"
            " CREATE ({start2})-[{rel2}:`{label}` {{`since`: ${since}}}]->({end2}:`Node` {{`name`: ${name4}}})"
            " SET {start}.`name` = ${name}, {end}.`name` = ${name2}, {rel}.`since` = ${since}, {start2}.`name` = ${name3}"
            " WHERE id({start}) = ${sid} AND id({end}) = ${eid} AND id({rel}) = ${rid} AND id({start2}) = ${sid2}"
            " RETURN {start}, {end}, {rel}, {start2}, {end2}, {rel2}".format(start=start.query_variable,
                sid=get_dict_key(params, sid), end=end.query_variable,
                eid=get_dict_key(params, eid), rel=rel.query_variable,
                label='Relationship', name=get_dict_key(params, name),
                name2=get_dict_key(params, name2), rid=get_dict_key(params, rid),
                since=get_dict_key(params, since), rel2=rel2.query_variable,
                name3=get_dict_key(params, name3), start2=start2.query_variable,
                sid2=get_dict_key(params, sid2), end2=end2.query_variable,
                name4=get_dict_key(params, name4)))

        self.assertEqual(exp, query)
        self.assertEqual(9, len(params))

    def test_can_delete_single_existing_relationship(self):
        _id = 999
        n = Node(id=_id)
        _id2 = 999
        n2 = Node(id=_id2)
        _id3 = 8989
        rel = Relationship(start=n, end=n2, id=_id3)
        q = Query(rel)
        query, params = q.delete()
        exp = "MATCH ()-[{var}:`{label}`]-() WHERE id({var}) = ${id} DELETE {var}".format(
            var=rel.query_variable, id=get_dict_key(params, _id3),
            label=rel.label)

        self.assertEqual(exp, query)
        self.assertEqual(1, len(params))

    def test_can_delete_multiple_existing_relationships(self):
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

        q = Query([rel, rel2])
        query, params = q.delete()
        exp = ("MATCH ()-[{var}:`{label}`]-() WHERE id({var}) = ${id}"
            " MATCH ()-[{var2}:`{label2}`]-() WHERE id({var2}) = ${id2}"
            " DELETE {var}, {var2}".format(var=rel.query_variable,
            id=get_dict_key(params, _id3),
            label=rel.label, var2=rel2.query_variable,
            id2=get_dict_key(params, _id4),
            label2=rel2.label))

        self.assertEqual(exp, query)
        self.assertEqual(2, len(params))


class RelationshipOutQueryTests(unittest.TestCase):
    direction = 'out'
    relationship_template = '-[{var}:`{label}`]->'

    class Start(Node):
        pass


    class End(Node):
        pass


    class Other(Node):
        pass


    class Knows(Relationship):
        pass


    def get_relationship(self, label, variable=''):
        return self.relationship_template.format(var=variable, label=label)

    def setUp(self):
        mapper = Mapper(connection=None)
        self.start_mapper = mapper.get_mapper(self.Start)
        self.end_mapper = mapper.get_mapper(self.End)

    def test_cannot_get_relationship_because_missing_context(self):
        self.start_mapper.reset()
        rq = RelationshipQuery(mapper=self.start_mapper,
            relationship_entity=self.Knows, single_relationship=False,
            direction=self.direction)
        
        def get():
            rq.query()
        
        self.assertRaises(RelatedQueryException, get)

    def test_can_get_realtionships_for_new_start_node(self):
        rq = RelationshipQuery(mapper=self.start_mapper,
            relationship_entity=self.Knows, direction=self.direction)
        start = self.start_mapper.create()
        self.start_mapper(start)

        query, params = rq.query()
        rel = self.get_relationship(self.Knows.labels[0])
        exp = 'MATCH (:`{start}`){rel}({end}) RETURN {end}'.format(
            start=start.labels[0], rel=rel,
            end=rq.other_end_key)
        self.assertEqual(exp, query)
        self.assertEqual(0, len(params))

    def test_can_get_realtionships_for_existing_start_node(self):
        rq = RelationshipQuery(mapper=self.start_mapper,
            relationship_entity=self.Knows, direction=self.direction)
        i_d = randint(10, 999)
        start = self.start_mapper.create(id=i_d)

        # set context
        self.start_mapper(start)

        query, params = rq.query()
        rel = self.get_relationship(self.Knows.labels[0])
        exp = ('MATCH ({start}){rel}({end})'
            ' WHERE id({start}) = ${id} RETURN {end}').format(
            start=start.query_variable, rel=rel,
            end=rq.other_end_key, id=get_dict_key(params, i_d))
        self.assertEqual(exp, query)
        self.assertEqual(1, len(params))

    def test_can_get_single_realtionship_for_new_start_node(self):
        rq = RelationshipQuery(mapper=self.start_mapper,
            relationship_entity=self.Knows, single_relationship=True,
            direction=self.direction)
        start = self.start_mapper.create()
        self.start_mapper(start)

        query, params = rq.query()
        rel = self.get_relationship(self.Knows.labels[0])
        exp = 'MATCH (:`{start}`){rel}({end}) RETURN {end} LIMIT 1'.format(
            start=start.labels[0], rel=rel,
            end=rq.other_end_key)

        self.assertEqual(exp, query)
        self.assertEqual(0, len(params))

    def test_can_get_single_realtionship_for_existing_start_node(self):
        rq = RelationshipQuery(mapper=self.start_mapper,
            relationship_entity=self.Knows, single_relationship=True,
            direction=self.direction)
        start = self.start_mapper.create()
        i_d = randint(10, 999)
        start = self.start_mapper.create(id=i_d)
        self.start_mapper(start)
        query, params = rq.query()
        rel = self.get_relationship(self.Knows.labels[0])
        exp = ('MATCH ({start}){rel}({end})'
            ' WHERE id({start}) = ${id} RETURN {end} LIMIT 1').format(
            start=start.query_variable, rel=rel,
            end=rq.other_end_key, id=get_dict_key(params, i_d))
        self.assertEqual(exp, query)
        self.assertEqual(1, len(params))

    def test_cannot_add_new_node_to_unrestricted_relationship(self):
        self.start_mapper.reset()
        rq = RelationshipQuery(mapper=self.start_mapper,
            relationship_entity=self.Knows, single_relationship=False,
            direction=self.direction)

        def add():
            end = self.end_mapper.create()
            rq.connect(end)

        self.assertRaises(RelatedQueryException, add)

    # note: The RelationshipQuery class utilizes the Query class and actual
    # query building tests are taken care of in
    # moesha.test.query.RelationshipQueryTests. No need to repeat it here


class RelationshipInQueryTests(RelationshipOutQueryTests):
    direction = 'in'
    relationship_template = '<-[{var}:`{label}`]-'


if __name__ == '__main__':
    unittest.main()
