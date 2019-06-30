from neo4j.v1 import GraphDatabase


class Connection(object):

    def __init__(self, host, port, username, password, protocol='bolt'):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.protocol = protocol
        self._driver = None

    @property
    def driver(self):
        if not self._driver:
            self._driver = GraphDatabase.driver(self.uri, auth=self.auth)

        return self._driver

    @property
    def uri(self):
        return '{}://{}:{}'.format(self.protocol, self.host, self.port)

    @property
    def auth(self):
        return (self.username, self.password)

    def query(self, query, params=None):
        params = params or {}

        with self.driver.session() as session:
            result = session.run(query, params)

            return Response(query=query, params=params, result=result)

    def cleanup(self):
        if self.driver:
            self.driver.close()
            self._driver = None


class Response(object):

    def __init__(self, query, params, result):
        self.query = query
        self.params = params
        self.result = result
        self._data = []
        self.result_data = self.result.data()

    @property
    def data(self):
        if self._data:
            return self._data

        for res in self.result_data:
            for var, ent in res.items():
                self._data.append(ent)

        return self._data
