echo 'batch.size=10' > producer.properties
echo 'partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner' >> producer.properties

kafka-topics --bootstrap-server worker1:9091 --create --topic Customers --replication-factor 2 --partitions 6
kafka-topics --bootstrap-server worker1:9091 --create --topic Transactions --replication-factor 2 --partitions 6
kafka-topics --bootstrap-server worker1:9091 --create --topic Items --replication-factor 2 --partitions 6
kafka-console-producer --broker-list worker1:9091 --topic Customers < /home/ubuntu/resources/Customers.csv
kafka-console-producer --broker-list worker1:9091 --topic Transactions < /home/ubuntu/resources/Transactions.csv
kafka-console-producer --broker-list worker1:9091 --topic Items < /home/ubuntu/resources/Items.csv

kafka-topics --bootstrap-server worker1:9091 --create --topic Customers_json --replication-factor 2 --partitions 6 --config segment.ms=30000
kafka-topics --bootstrap-server worker1:9091 --create --topic Transactions_json --replication-factor 2 --partitions 6
kafka-topics --bootstrap-server worker1:9091 --create --topic Items_json --replication-factor 2 --partitions 6
kafka-console-producer --broker-list worker1:9091 --topic Customers_json --producer.config producer.properties < /home/ubuntu/resources/Customers_json.txt
kafka-console-producer --broker-list worker1:9091 --topic Transactions_json < /home/ubuntu/resources/Transactions_json.txt
kafka-console-producer --broker-list worker1:9091 --topic Items_json < /home/ubuntu/resources/Items_json.txt

kafka-topics --bootstrap-server alt-worker1:10091 --create --topic Alt_customers_json --replication-factor 1 --partitions 6
kafka-console-producer --broker-list alt-worker1:10091 --topic Alt_customers_json --producer.config producer.properties < /home/ubuntu/resources/Customers_json.txt