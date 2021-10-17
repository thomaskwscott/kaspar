echo 'batch.size=10' > producer.properties
echo 'partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner' >> producer.properties

sleep 30
kafka-console-producer --broker-list worker1:9091 --topic Customers_json --producer.config producer.properties < /home/ubuntu/sampleData/Customers_dummy_json.txt
