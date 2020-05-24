kafka-topics --bootstrap-server worker1:9091 --create --topic testTopic --replication-factor 3 --partitions 1
kafka-topics --bootstrap-server worker1:9091 --create --topic testTopic2 --replication-factor 3 --partitions 1
seq 100 | kafka-console-producer --broker-list worker1:9091 --topic testTopic
seq 25 | kafka-console-producer --broker-list worker1:9091 --topic testTopic2