# Kaspar Server

A REST interface for running kaspar queries and retrieving results.

## How it works

Kaspar Server runs as a Spark application and serves REST APIs using akka-http. Results are stored in a MySQL database.

## Running the server

the main project has a wrapper to launch the server:

```
./launchServer.sh ../kaspar_server/src/main/resources/kaspar.properties
```

## Requests

The following apis exist right now:

get the version:
```
curl -X GET localhost:8888/version
```

submit a query (this returns the query id):
```
curl -X POST localhost:8888/query --data-binary @/home/ubuntu/kaspar_server/src/main/resources/test_statement.sql
```

fetch results (use the above id as a parameter):
```
curl -X GET localhost:8888/result?query_id=74cc7f28_6c7c_407c_8a3c_8ddf4f33d2a2
```
