#! /bin/bash
echo "{ \"customer_id\": `expr $RANDOM % 10`, \"item_id\": `expr $RANDOM % 14` }" |  kafka-console-producer --broker-list worker1:9091 --topic Transactions_json
echo "produced new transaction at `date`"
