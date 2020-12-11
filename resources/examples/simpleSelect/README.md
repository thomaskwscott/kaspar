# Simple select * query  with SparkSQL

This demo loads messages from 2 topics that store JSON messages and displays them using sparkSQL

## Input topics

1. Customers_json - customer id, name, address and age
2. Items_json - item id, name and price

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
