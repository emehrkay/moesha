from neo4j.v1 import GraphDatabase
from pypher.builder import Pypher

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "test"))

p = Pypher()
p.CREATE.node('user', 'User', Name='Jim').RETURN.user

print(str(p), p.bound_params)

with driver.session() as  session:
    result = session.run(str(p), **p.bound_params)

    print(result.data())
