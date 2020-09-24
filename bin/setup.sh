kafka-topics --bootstrap-server worker1:9091 --create --topic Customers --replication-factor 2 --partitions 6
kafka-topics --bootstrap-server worker1:9091 --create --topic Transactions --replication-factor 2 --partitions 6
kafka-topics --bootstrap-server worker1:9091 --create --topic Items --replication-factor 2 --partitions 6
kafka-console-producer --broker-list worker1:9091 --topic Customers < /home/ubuntu/resources/Customers.csv
kafka-console-producer --broker-list worker1:9091 --topic Transactions < /home/ubuntu/resources/Transactions.csv
kafka-console-producer --broker-list worker1:9091 --topic Items < /home/ubuntu/resources/Items.csv
kafka-topics --bootstrap-server worker1:9091 --create --topic Customers_json --replication-factor 2 --partitions 6
kafka-topics --bootstrap-server worker1:9091 --create --topic Transactions_json --replication-factor 2 --partitions 6
kafka-topics --bootstrap-server worker1:9091 --create --topic Items_json --replication-factor 2 --partitions 6
kafka-console-producer --broker-list worker1:9091 --topic Customers_json < /home/ubuntu/resources/Customers_json.txt
kafka-console-producer --broker-list worker1:9091 --topic Transactions_json < /home/ubuntu/resources/Transactions_json.txt
kafka-console-producer --broker-list worker1:9091 --topic Items_json < /home/ubuntu/resources/Items_json.txt