#!/bin/bash
# Register the Neo4j Sink Connector by exec-ing into the connector pod.
#
# Neo4j credentials: set NEO4J_PASSWORD in your environment. It defaults to
# the local-development value used when this pipeline was built.

set -e

NEO4J_PASSWORD="${NEO4J_PASSWORD:-processingpipeline}"

# Get the connector pod name
POD=$(kubectl get pods -l app=kafka-neo4j-connector -o jsonpath='{.items[0].metadata.name}')
echo "==> Using pod: $POD"

echo "==> Waiting for Kafka Connect REST API inside pod to be ready..."
until kubectl exec "$POD" -- curl -sf http://localhost:8083/connectors > /dev/null 2>&1; do
  echo "    Not ready yet, retrying in 5s..."
  sleep 5
done
echo "==> Kafka Connect is ready."

echo "==> Registering Neo4j sink connector..."
kubectl exec "$POD" -- curl -X POST http://localhost:8083/connectors \
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
echo "==> Checking connector status..."
sleep 3
kubectl exec "$POD" -- curl -sf http://localhost:8083/connectors/neo4j-sink/status
echo ""
echo "==> Done."
