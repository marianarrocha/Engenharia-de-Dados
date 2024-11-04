%sh
./kafka_2.12-3.6.2/bin/kafka-topics.sh --create --topic meu-topico --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
