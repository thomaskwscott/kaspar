kafka-topics --bootstrap-server worker1:9091 --create --topic Customers --replication-factor 3 --partitions 6
kafka-topics --bootstrap-server worker1:9091 --create --topic Transactions --replication-factor 3 --partitions 6
kafka-topics --bootstrap-server worker1:9091 --create --topic Items --replication-factor 3 --partitions 6
kafka-console-producer --broker-list worker1:9091 --topic Customers < /home/ubuntu/resources/Customers.csv
kafka-console-producer --broker-list worker1:9091 --topic Transactions < /home/ubuntu/resources/Transactions.csv
kafka-console-producer --broker-list worker1:9091 --topic Items < /home/ubuntu/resources/Items.csv
