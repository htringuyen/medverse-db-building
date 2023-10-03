from neo4j import GraphDatabase as graphdb


URI = "bolt://localhost:7687"
USER = "snowj"
PASSWORD = "abcd1234"
DATABASE = "medverse"

class Neo4jConnection(object):
    def __init__(self):
        self._driver = graphdb.driver(
            URI, auth=(USER, PASSWORD), encrypted=False, database=DATABASE)

    def close(self):
        self._driver.close()

    def run_query(self, query):
        with self._driver.session() as session:
            result = session.run(query).consume()
        return result

    @staticmethod
    def do_cypher_tx(tx, cypher):
        result = tx.run(cypher)
        values = [record.values() for record in result]
        return values

    def execute_read_query(self, query):
        with self._driver.session() as session:
            values = session.execute_read(self.do_cypher_tx, query)
        return values