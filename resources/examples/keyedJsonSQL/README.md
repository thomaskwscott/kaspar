# Loading JSON messages and querying them with SparkSQL

This demo loads messages from 3 topics that store JSON messages and joins them using sparkSQL. On 
one of the input topics some columns are stored in the message key and some are in the message 
value. This demo differs from the jsonSQL demo in that the Items table is loaded with the 
SimpleJsonKeyValueColumnifier so that it can use the message key.  

## Input topics

1. Customers_json - customer id, name, address and age
2. Items_json - item id, name and price
3. Transactions_json - customer id and item id

## Setup

start the demo environment with:

```
docker-compose up -d
```

from within the master container run:
 
 ```
/home/ubuntu/bin/setup.sh
```

to populate the topics.

## Running

run:

```
/home/ubuntu/bin/launchShell.sh
```

to start Spark shell. After this copy the contents of run_me.scala into the shell to run the demo.