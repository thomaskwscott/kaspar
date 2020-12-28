# Loading JSON messages and querying them with SparkSQL

This demo loads messages from 3 topics that store JSON messages and joins them using sparkSQL. This 
demo differs from the jsonSql demo because it uses the PathJsonValueColumnifier to allow complex 
mappings between json fields and column values. In this case the jsonPath value $.internal.margin 
is used to represent the margin made from each sale.

## Input topics

1. Customers_json - customer id, name, address and age
2. Items_json - item id, name, price and the margin made on this item
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
