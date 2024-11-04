%sh
sudo wget https://downloads.apache.org/kafka/3.6.2/kafka_2.12-3.6.2.tgz

%sh
tar -xvf kafka_2.12-3.6.2.tgz

%sh
./kafka_2.12-3.6.2/bin/zookeeper-server-start.sh ./kafka_2.12-3.6.2/config/zookeeper.properties
