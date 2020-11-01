# Loading CSV messages and querying them with SparkSQL

This demo loads messages from 3 topics that store CSV messages and joins them using sparkSQL

## Input topics

1. Customers - customer id, name, address and age
2. Items - item id, name and price
3. Transactions - customer id and item id

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
