"""Graph analytics using the Neo4j Graph Data Science (GDS) library.

Alternative to cypher_analytics.py: instead of hand-rolled iterative Cypher,
this implementation projects an in-memory graph and calls GDS procedures
for PageRank, with shortest-path BFS expressed in Cypher.

Credentials are passed in by the caller — no secrets are hardcoded here.
"""
from neo4j import GraphDatabase


class Interface:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self._driver.verify_connectivity()

    def close(self):
        self._driver.close()

    def bfs(self, start_node, last_node):
        with self._driver.session() as session:
            result = session.run("""
                MATCH (start:Location {name: $start_node}), (target:Location {name: $last_node})
                MATCH p = shortestPath((start)-[:TRIP*]->(target))
                RETURN [node IN nodes(p) | {name: node.name}] AS path
            """, start_node=start_node, last_node=last_node).single()

            if result is None or result["path"] is None:
                return []

            return [{"path": result["path"]}]

    def pagerank(self, max_iterations, weight_property):
        graph_name = "pagerankGraph"

        with self._driver.session() as session:
            session.run("""
                CALL gds.graph.exists($graph_name)
                YIELD exists
                WITH exists
                WHERE exists
                CALL gds.graph.drop($graph_name, false)
                YIELD graphName
                RETURN graphName
            """, graph_name=graph_name)

            session.run("""
                CALL gds.graph.project(
                    $graph_name,
                    'Location',
                    {
                        TRIP: {
                            type: 'TRIP',
                            orientation: 'NATURAL',
                            properties: $weight_property
                        }
                    }
                )
            """, graph_name=graph_name, weight_property=weight_property)

            rows = list(session.run("""
                CALL gds.pageRank.stream($graph_name, {
                    maxIterations: $max_iterations,
                    dampingFactor: 0.85,
                    relationshipWeightProperty: $weight_property
                })
                YIELD nodeId, score
                WITH gds.util.asNode(nodeId) AS node, score
                RETURN node.name AS name, score
                ORDER BY score DESC, name ASC
            """, graph_name=graph_name,
                 max_iterations=max_iterations,
                 weight_property=weight_property))

            session.run("""
                CALL gds.graph.exists($graph_name)
                YIELD exists
                WITH exists
                WHERE exists
                CALL gds.graph.drop($graph_name, false)
                YIELD graphName
                RETURN graphName
            """, graph_name=graph_name)

            if not rows:
                return {}, {}

            max_result = {
                "name": rows[0]["name"],
                "score": rows[0]["score"]
            }

            min_result = {
                "name": rows[-1]["name"],
                "score": rows[-1]["score"]
            }

            return max_result, min_result
