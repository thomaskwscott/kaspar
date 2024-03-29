echo 'batch.size=10' > producer.properties
echo 'partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner' >> producer.properties

kafka-topics --bootstrap-server worker1:9091 --create --topic Customers --replication-factor 2 --partitions 6
kafka-topics --bootstrap-server worker1:9091 --create --topic Transactions --replication-factor 2 --partitions 6
kafka-topics --bootstrap-server worker1:9091 --create --topic Items --replication-factor 2 --partitions 6
kafka-console-producer --broker-list worker1:9091 --topic Customers < /home/ubuntu/resources/Customers.csv
kafka-console-producer --broker-list worker1:9091 --topic Transactions < /home/ubuntu/resources/Transactions.csv
kafka-console-producer --broker-list worker1:9091 --topic Items < /home/ubuntu/resources/Items.csv

kafka-topics --bootstrap-server worker1:9091 --create --topic Customers_json --replication-factor 2 --partitions 1 --config segment.ms=30000
kafka-topics --bootstrap-server worker1:9091 --create --topic Customers_volume_json --replication-factor 3 --partitions 6
kafka-topics --bootstrap-server worker1:9091 --create --topic Transactions_json --replication-factor 2 --partitions 6
kafka-topics --bootstrap-server worker1:9091 --create --topic Items_json --replication-factor 2 --partitions 6
kafka-console-producer --broker-list worker1:9091 --topic Customers_json --producer.config producer.properties < /home/ubuntu/resources/Customers_json.txt
kafka-console-producer --broker-list worker1:9091 --topic Customers_volume_json --producer.config producer.properties < /home/ubuntu/resources/Customers_json.txt
kafka-console-producer --broker-list worker1:9091 --topic Transactions_json < /home/ubuntu/resources/Transactions_json.txt
kafka-console-producer --broker-list worker1:9091 --topic Items_json --property "parse.key=true" --property "key.separator=|" < /home/ubuntu/resources/Items_json.txt

#kafka-topics --bootstrap-server alt-worker1:10091 --create --topic Alt_customers_json --replication-factor 1 --partitions 6
#kafka-console-producer --broker-list alt-worker1:10091 --topic Alt_customers_json --producer.config producer.properties < /home/ubuntu/resources/Customers_json.txt

# to set up index testing we have to dome tricky stuff:
# 1. a segment with separate record batches in order dummy, good, dummy
# 2. roll the segment then add a further dummy as indexing doesn't happen on the active segment
kafka-topics --bootstrap-server worker1:9091 --create --topic Customers_index --replication-factor 2 --partitions 1 --config segment.ms=30000
kafka-console-producer --broker-list worker1:9091 --topic Customers_index --producer.config producer.properties < /home/ubuntu/resources/Single_Customer_Dummy.txt
kafka-console-producer --broker-list worker1:9091 --topic Customers_index --producer.config producer.properties < /home/ubuntu/resources/Single_Customer_Legit.txt
kafka-console-producer --broker-list worker1:9091 --topic Customers_index --producer.config producer.properties < /home/ubuntu/resources/Single_Customer_Dummy.txt
sleep 30
kafka-console-producer --broker-list worker1:9091 --topic Customers_index --producer.config producer.properties < /home/ubuntu/resources/Single_Customer_Dummy2.txt