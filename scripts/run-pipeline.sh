#!/bin/bash
# End-to-end run: redeploy the Kafka Connect worker, register the Neo4j sink
# connector, port-forward services, stream data, and verify the graph.
#
# Neo4j credentials: set NEO4J_PASSWORD in your environment. It defaults to
# the local-development value used when this pipeline was built.
set -e

NEO4J_PASSWORD="${NEO4J_PASSWORD:-processingpipeline}"

echo ""
echo "===== STEP 1: Redeploy connector ====="
kubectl delete deployment kafka-neo4j-connector --ignore-not-found
kubectl apply -f k8s/kafka-neo4j-connector.yaml

echo ""
echo "===== STEP 2: Waiting for connector pod to be Running ====="
kubectl rollout status deployment/kafka-neo4j-connector --timeout=120s

POD=$(kubectl get pods -l app=kafka-neo4j-connector -o jsonpath='{.items[0].metadata.name}')
echo "Pod: $POD"

echo ""
echo "===== STEP 3: Waiting for Kafka Connect REST API (up to 3 min) ====="
for i in $(seq 1 36); do
  STATUS=$(kubectl exec "$POD" -- curl -s -o /dev/null -w "%{http_code}" http://localhost:8083/ 2>/dev/null || echo "000")
  echo "  Attempt $i: HTTP $STATUS"
  if [ "$STATUS" = "200" ]; then
    echo "  Connect REST API is ready!"
    break
  fi
  sleep 5
done

echo ""
echo "===== STEP 4: Register Neo4j sink connector ====="
kubectl exec "$POD" -- curl -s -X DELETE http://localhost:8083/connectors/neo4j-sink 2>/dev/null || true
sleep 2

kubectl exec "$POD" -- curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"neo4j-sink\",
    \"config\": {
      \"connector.class\": \"streams.kafka.connect.sink.Neo4jSinkConnector\",
      \"topics\": \"nyc_taxicab_data\",
      \"neo4j.server.uri\": \"bolt://neo4j-service:7687\",
      \"neo4j.authentication.basic.username\": \"neo4j\",
      \"neo4j.authentication.basic.password\": \"${NEO4J_PASSWORD}\",
      \"neo4j.topic.cypher.nyc_taxicab_data\": \"MERGE (t:Trip {puLocationId: event.PULocationID, doLocationId: event.DOLocationID}) SET t.tripDistance = event.trip_distance, t.fareAmount = event.fare_amount\",
      \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
      \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
      \"value.converter.schemas.enable\": \"false\",
      \"errors.tolerance\": \"all\",
      \"errors.log.enable\": \"true\",
      \"errors.log.include.messages\": \"true\"
    }
  }"

echo ""
sleep 3
echo "===== STEP 5: Connector status ====="
kubectl exec "$POD" -- curl -s http://localhost:8083/connectors/neo4j-sink/status

echo ""
echo "===== STEP 6: Port-forward Kafka (background) ====="
pkill -f "port-forward svc/kafka-service" 2>/dev/null || true
kubectl port-forward svc/kafka-service 9092:9092 &
PF_PID=$!
echo "Port-forward PID: $PF_PID"
sleep 3

echo ""
echo "===== STEP 7: Run data producer ====="
python3 producer/data_producer.py

echo ""
echo "===== STEP 8: Port-forward Neo4j (background) ====="
pkill -f "port-forward svc/neo4j" 2>/dev/null || true
kubectl port-forward svc/neo4j-release 7687:7687 7474:7474 &
sleep 3

echo ""
echo "===== STEP 9: Verify nodes in Neo4j ====="
NEO4J_PASSWORD="${NEO4J_PASSWORD}" python3 - <<'EOF'
import os
from neo4j import GraphDatabase
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", os.environ["NEO4J_PASSWORD"]))
with driver.session() as s:
    count = s.run("MATCH (n) RETURN count(n) AS c").single()["c"]
    print(f"Nodes in Neo4j: {count}")
    if count > 0:
        sample = s.run("MATCH (n) RETURN n LIMIT 1").single()["n"]
        print(f"Sample node: {dict(sample)}")
driver.close()
EOF

echo ""
echo "===== DONE ====="
echo "Neo4j Browser: http://localhost:7474  (user: neo4j)"
