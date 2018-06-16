from neo4j.v1 import GraphDatabase
from pprint import pprint

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "test"))

def x(q,  params=None):
    params = params or {}
    response = []
    with driver.session() as session:
        with session.begin_transaction() as tx:
            for record in tx.run(q, **params):
                response.append(record)


    return response

q = 'create (n:Person {name: "mmmm"})-[r:Knows:Random {since: "yeseterday"}]->() return r, n, r.since'
q = 'return 1 as `iiii one`'
q = """
create (m {name: "SOME NAME"})
match (n)
where id(n) = 100 or n.name = "mark"
set n.name = 'MARK HENDERSON'
return n, m"""
q = '''
match (x) where id(x) = 1
create (x)-[r:KNOWS]->(n:Actor)-[:KNOWS]->(x)
return r, x, n'''
q = 'match (n {name: $n1}) return n'
p = {'n1': 'name', 'n2': '999'}
q = 'CREATE (n0 {`name`: $n0_name_0}), (n1 {`name`: $n1_name_0}) RETURN n0, n1'
p = {'n0_name_0': 'mark', 'n1_name_0': 'markxxx'}
q = 'match (n) WHERE id(n) = $i set n.`first_name` = $f  return n'
p = {'f': 'some first name', 'i': 63}
q = "MATCH (n) WHERE id(n) = $id MATCH (n2) WHERE id(n2) = $id2 SET n.`name` = $val, n2.`name` = $val2 RETURN n, n2"
p = {'id': 1, 'val': 'val one', 'id2': 2, 'val2': 'val two'}
q2 = 'match (n) where id(n) = 1 match (n2) where id(n2) = 2 return n, n2'
q = 'match (n) return n'
q = """
match (n)
where id(n) = 1
create (n)-[r:knows]->(n2 {name: '44444444444444'})
set n.age = 9999, n2.sex = 'ooooo'
return n, r, n2
"""
q = 'match ()-[r]-() return id(r) order by id(r)'
q = "match(n)-[r]-() return n, r, 3 limit 20"
# q = 'match ()-[r]-() where id(r) = 6 match ()-[r2]-() where id(r2) = 7 delete r, r2'
# q = 'CALL dbms.listQueries() YIELD queryId, username, query, elapsedTimeMillis, requestUri, status'
q = 'match (n) return n {.name, .age, .sex, `country!!!`: $country}'
p = {'country': "888888", 'name': '.name'}
r = x(q, p)

# q = 'MATCH (n13) WHERE id(n13) = $n13_id_0 MATCH (n1) WHERE id(n1) = $n13_id_1 DETACH DELETE n13, n1'
# p = {'n13_id_0':0, 'n13_id_1': 1}
# r = x(q, p)
# import pudb; pu.db

pprint(r)
print(type(r))

