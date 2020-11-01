# Applying predicates to loaded messages

This demo loads a single topic with various predicates applied at both a row and segment level

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
from outside the docker environment run:

```
./make_index.sh
```

This populates index files alongside the segment files for the Customers topic. Eventually this will be done automatically.

## Running

run:

```
/home/ubuntu/bin/launchShell.sh
```

to start Spark shell. After this copy the contents of run_me.scala into the shell to run the demo.
