# Kafka commands with docker

## Create topic in kafka cluster
### Execute command for create topic with replication factor and partition number
docker exec -it kafka kafka-topics \
--bootstrap-server localhost:9092 \
--create --topic word-count-output \
--replication-factor 1 \
--partitions 1

## Produce messages
### Execute command for producing messages
docker exec -it kafka kafka-console-producer \
--bootstrap-server localhost:9092 \
--topic word-count-input

## Consume messages
### Execute command for consume messages
docker exec -it kafka kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic word-count-output
--from-beginning \
--property print.key=true \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer


#### All messages
--from-beginning
#### With key for partitioning
--property "key.separator=-" --property "parse.key=true"
#### With group id
--group group-course-1
#### With headers 
--property "print.headers=true" --property "print.timestamp=true" 

## Another commands for Kafka cluster
### List topics
docker exec -it kafka kafka-topics \
--bootstrap-server localhost:9092 \
--list
### Describe topic 
docker exec -it kafka kafka-topics \
--bootstrap-server localhost:9092 \
--describe \
--topic test-topic-1
### List consumer groups
docker exec -it kafka kafka-consumer-groups \
--bootstrap-server localhost:9092 \
--list


