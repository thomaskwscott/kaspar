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

### Version API:
```
curl -X GET localhost:8888/version
```

### Column API:

this manipulates columns in the metastore

Create a column
```
curl -X POST localhost:8888/column -d '{"columnName":"someColumn", "tableName":"someTable","columnType":"Integer"}'
```
Get all columns in a table
```
curl -X GET localhost:8888/column?tableName=someTable
```
Get a specific column by id
```
curl -X GET localhost:8888/column?columnId=1
```
Get all columns
```
curl -X GET localhost:8888/column
```
Delete a column
```
curl -X DELETE localhost:8888/column?columnId=1
```

### Query API:

this submits and monitors the status of queries

Submit a query
```
curl -X POST localhost:8888/query --data-binary @/home/ubuntu/kaspar_server/src/main/resources/test_statement_implicit_cols.sql
curl -X POST localhost:8888/query -d 'SELECT * FROM Customers_json'
```
Get the status of a particular query
```
curl -X GET localhost:8888/query?queryId=74cc7f28_6c7c_407c_8a3c_8ddf4f33d2a2
```
Get all submitted queries
```
curl -X GET localhost:8888/query
```

Result API:

this fetches results

```
curl -X GET localhost:8888/result?queryId=74cc7f28_6c7c_407c_8a3c_8ddf4f33d2a2
curl -X GET localhost:8888/result
curl -X DELETE localhost:8888/result?queryId=74cc7f28_6c7c_407c_8a3c_8ddf4f33d2a2
```

## Explicit vs Implicit Columns

Kaspar supports 2 ways of dictating the columns to be used in the query: 

1. Explicit Columns - these are specified as comment lines above the sql in the query submitted to Kaspar:

    ```
    # col: Customers_json.id Integer
    # col: Customers_json.name String
    # col: Customers_json.address String
    # col: Customers_json.age Integer
    SELECT * from Customers_json
    ```
2. Implicit Columns - these are pre created using the columns API and inferred from table names in the submitted SQL.

## Tutorial

Using implicit columns we'll submit a simple select * to Kaspar and monitor it.

1. Create columns:

    ```
    curl -X POST localhost:8888/column --data '{"tableName":"Customers_json","columnName":"id","columnType":"Integer"}'
    curl -X POST localhost:8888/column --data '{"tableName":"Customers_json","columnName":"name","columnType":"String"}'
    curl -X POST localhost:8888/column --data '{"tableName":"Customers_json","columnName":"address","columnType":"String"}'
    curl -X POST localhost:8888/column --data '{"tableName":"Customers_json","columnName":"age","columnType":"Integer"}'
    curl -X GET localhost:8888/column
    ```
2. Submit a simple query:

    ```
    curl -X POST localhost:8888/query -d 'SELECT * FROM Customers_json'
    ```

3. See the status of our query and fetch the query id:

    ```
    curl -X GET localhost:8888/query
    ```

4. When the above shows the query as complete we fetch the results:
    
    ```
    curl -X GET localhost:8888/result?queryId=74cc7f28_6c7c_407c_8a3c_8ddf4f33d2a2
    ```

