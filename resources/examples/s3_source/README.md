# Loading JSON messages from S3 and querying them with SparkSQL

This demo loads messages from 3 topics that store CSV messages and joins them using sparkSQL

## Input topics

1. Items_json - item id, name and price
2. Transactions_json - customer id and item id

## Other input data

Customers - customer id, name, address and age

This data is stored in a segment file in S3, this is designed to simulate tiered storage in kafka

## Setup

1. First prepare the S3 data by copying `resources/00000000000000000000.log` into an S3 bucket named `kaspar` in region `eu-west-1`
2. export credentials for accessing the bucket as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`

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
