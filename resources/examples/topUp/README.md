# Loading JSON messages then topping them up

This demo loads messages from a single topic that stores JSON data. Then tops it up using both segment and row predicates.

## Input topics

1. Customers_json - customer id, name, address and age

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
