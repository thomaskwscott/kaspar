echo 'batch.size=10' > producer.properties
echo 'partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner' >> producer.properties

kafka-topics --bootstrap-server worker1:9091 --create --topic Customers --replication-factor 2 --partitions 6
kafka-topics --bootstrap-server worker1:9091 --create --topic Transactions --replication-factor 2 --partitions 6
kafka-topics --bootstrap-server worker1:9091 --create --topic Items --replication-factor 2 --partitions 6
kafka-console-producer --broker-list worker1:9091 --topic Customers < /home/ubuntu/sampleData/Customers.csv
kafka-console-producer --broker-list worker1:9091 --topic Transactions < /home/ubuntu/sampleData/Transactions.csv
kafka-console-producer --broker-list worker1:9091 --topic Items < /home/ubuntu/sampleData/Items.csv

kafka-topics --bootstrap-server worker1:9091 --create --topic Customers_json --replication-factor 2 --partitions 1 --config segment.ms=10000
kafka-topics --bootstrap-server worker1:9091 --create --topic Transactions_json --replication-factor 2 --partitions 6
kafka-topics --bootstrap-server worker1:9091 --create --topic Items_json --replication-factor 2 --partitions 6
kafka-console-producer --broker-list worker1:9091 --topic Customers_json --producer.config producer.properties < /home/ubuntu/sampleData/Customers_json.txt
kafka-console-producer --broker-list worker1:9091 --topic Transactions_json < /home/ubuntu/sampleData/Transactions_json.txt
kafka-console-producer --broker-list worker1:9091 --topic Items_json --property "parse.key=true" --property "key.separator=|" < /home/ubuntu/sampleData/Items_json.txt
