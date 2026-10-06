"""Graph analytics over the streamed trip data, implemented in pure Cypher.

Reads the NEO4J_PASSWORD environment variable; falls back to the local
development default used when the pipeline was built.
"""
import os

from neo4j import GraphDatabase

# Neo4j connection details
URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = os.environ.get("NEO4J_PASSWORD", "processingpipeline")

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))


def pagerank(iterations=20, dampingFactor=0.85):
    """
    Run PageRank on the Location graph using pure Cypher.
    Returns a list of (locationId, score) sorted by score descending.
    """
    with driver.session() as session:
        # Initialize all nodes with rank 1.0
        session.run("MATCH (n:Location) SET n.rank = 1.0")

        # Get total number of nodes
        total = session.run("MATCH (n:Location) RETURN count(n) AS count").single()["count"]

        # Iterative PageRank
        for i in range(iterations):
            session.run("""
                MATCH (n:Location)
                SET n.newRank = $minRank
            """, minRank=(1 - dampingFactor) / total if total > 0 else 0)

            session.run("""
                MATCH (src:Location)-[:TRIP]->(dst:Location)
                WITH dst, sum(src.rank / size([(src)-[:TRIP]->() | 1])) AS contribution
                SET dst.newRank = dst.newRank + $dampingFactor * contribution
            """, dampingFactor=dampingFactor)

            session.run("MATCH (n:Location) SET n.rank = n.newRank")

        # Get results
        result = session.run("""
            MATCH (n:Location)
            RETURN n.name AS locationId, n.rank AS score
            ORDER BY score DESC
        """)

        records = [(record["locationId"], record["score"]) for record in result]

        # Clean up temporary properties
        session.run("MATCH (n:Location) REMOVE n.rank, n.newRank")

        return records


def bfs(start_node, target_node):
    """
    Run Breadth-First Search from start_node to target_node using pure Cypher.
    Returns the shortest path as a list of location IDs, or empty list if no path.
    """
    with driver.session() as session:
        result = session.run("""
            MATCH path = shortestPath(
                (start:Location {name: $start})-[:TRIP*]-(end:Location {name: $end})
            )
            RETURN [node in nodes(path) | node.name] AS pathNodes
        """, start=start_node, end=target_node)

        record = result.single()
        return record["pathNodes"] if record else []


if __name__ == "__main__":
    print("Running PageRank...")
    pr_results = pagerank()
    print("Top 5 locations by PageRank:")
    for loc, score in pr_results[:5]:
        print(f"  Location {loc}: {score:.4f}")

    print("\nRunning BFS from location 3 to location 213...")
    path = bfs(3, 213)
    print(f"Path: {path}")

    driver.close()
