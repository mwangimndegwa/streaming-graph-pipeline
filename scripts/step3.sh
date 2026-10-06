#!/bin/bash
# Minimal connector registration (used during development).
# For the full supervised run, use scripts/run-pipeline.sh instead.
#
# Neo4j credentials: set NEO4J_PASSWORD in your environment.
set -e

NEO4J_PASSWORD="${NEO4J_PASSWORD:-processingpipeline}"

# Wait for Kafka Connect to be ready
sleep 30

# Register the Neo4j sink connector
curl -X POST http://localhost:8083/connectors/ \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"Neo4jSinkConnectorJSONString\",\"config\":{\"topics\":\"nyc_taxicab_data\",\"connector.class\":\"streams.kafka.connect.sink.Neo4jSinkConnector\",\"key.converter\":\"org.apache.kafka.connect.json.JsonConverter\",\"key.converter.schemas.enable\":\"false\",\"value.converter\":\"org.apache.kafka.connect.json.JsonConverter\",\"value.converter.schemas.enable\":\"false\",\"errors.retry.timeout\":\"-1\",\"errors.retry.delay.max.ms\":\"1000\",\"errors.tolerance\":\"all\",\"errors.log.enable\":\"true\",\"errors.log.include.messages\":\"true\",\"neo4j.server.uri\":\"bolt://neo4j-service:7687\",\"neo4j.authentication.basic.username\":\"neo4j\",\"neo4j.authentication.basic.password\":\"${NEO4J_PASSWORD}\",\"neo4j.topic.cypher.nyc_taxicab_data\":\"MERGE (p:Location {name: toInteger(event.PULocationID)}) MERGE (d:Location {name: toInteger(event.DOLocationID)}) MERGE (p)-[:TRIP {distance: toFloat(event.trip_distance), fare: toFloat(event.fare_amount)}]->(d)\",\"consumer.auto.offset.reset\":\"earliest\"}}"
