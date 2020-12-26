# Continually running a simple query as new messages are added to source topics. 

This demo performs the query from the jsonSQL demo. As part of the demo new transactions are added to the 
Transactions_json source topic and the joins from the query are performed continuously. 

## Input topics

1. Customers_json - customer details, this topic content is kept static during the demo. 
2. Items_json - item details, this topic content is kept static during the demo.
3. Transactions_json - A transaction corresponds to a customer buying an item (i.e. customer_id, item_id). During this 
demo we add new transactions and output more useful details (customer name, item name and item price). 

## Setup

start the demo environment with:

```
docker-compose up -d
```

run:
 
 ```
/home/ubuntu/bin/setup.sh
```

to populate the topics.

## Running

from within the master container run:

```
/home/ubuntu/bin/launchShell.sh
```

to start Spark shell. After this copy the contents of run_me.scala into the shell to run the demo.

to add new transactions run a second session on the master container and run:

```
./home/ubuntu/resources/genTransaction.sh
```


