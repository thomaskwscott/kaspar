.. _kaspar_core:

Kaspar Core
===========

Kaspar is a way to bring Spark's computational capabilities closer to Kafka data than the
traditional consume pattern. Spark resources and Kafka resources are co located to provide fast
access to the underlying Kafka data without going over the network.

How it works
------------

---------------------
The data load process
---------------------

 1. Using a Kafka AdminClient, Kaspar determines the node and disk location of log segments
    relevant to the Kaspar query. These locations are added to a Spark RDD that is partitioned
    so that each entry will run as it's own task.
 2. Spark schedules each task to a worker colocated with a broker. Spark's preferredLocation
    features are used to guide this scheduling however this is not garunteed. For this reason, the
    first thing this task does is check executor's host name against the kafka partition's ISR
    host list (This does not have to be leader as Kaspar can satisfy it's read requirements from
    replicas. If the executor host is not in the ISR list then the task is failed and will be
    rescheduled by Spark until it finds an appropriate host to execute on.
 3. Once the above is satisfied, the task will read the data directly from disk (using a method
    similar to Kafka's dumpLogSegments command) and return a RDD containing the message values.
 4. From here on in Spark can do the rest, possibly  adding a schema and running SparkSQL?

---------------------------
Creating rows from messages
---------------------------

Messages are converted to RawRow (kaspar.dataload.structure.RawRow) objects which wrap an array of
values that each represent a column making a row. This conversion happens during the read from disk
process. This is done using a RowDeserializer (kaspar.dataload.structure.RowDeserializer). The
first 3 columns (rawRow.getColumnVal(0), rawRow.getColumnVal(1) and rawRow.getColumnVal(2)) are
currently reserved for partition, offset and timestamps of the message.

RawRows are fetched using the `KasparDriver.getRows()` method. This takes a number of parameters
relevant to the underlying Kafka topic, any predicates to be applied at read time and the
RowDeserializer used to convert messages to RawRow objects.

For more information on RowDeserializers please see:
:ref:`Row Deserializers <kaspar_rowdeserializers>`

----------------
Using Predicates
----------------

Kaspar supports applying predicates at data read time at 2 levels:

  * Row predicates - a filter that is applied to each row read, unmatched rows are discarded.
  * Segment predicates - a filter that determines whether or not a whole log segment should be
    read. These typically take advantage nof indexes provided for the segment files. For instance,
    if an index indicates that the maximum value for a given field in the segment is smaller than
    the minimum value in your query then there is no need to  read this segment. These predicates
    can greatly increase performance.

For more information on predicates see:
:ref:`Predicate Generators <kaspar_predicates>`

-----------
Running SQL
-----------

Using SparkSQL we can load the RDDs created above as tables that can then ber queried using
SparkSQL. The `KasparDriver.registerTable()` method is provided for this. This takes the following
arguments:

  * sqlContext - A SparkSQlContext in whcih to register the table
  * rawRdd - The RDD created above
  * tableName - The table name
  * columnMappings - A list of tuples each representing a column name and column type. The order
    of this list should match the order of the fields in the RDD.

Building and Running
--------------------

Kaspar is a maven project so supports the usual goals. However, due to the way in which they are
run, integration tests must be excluded from this build and run separately:

.. sourcecode:: bash

    mvn -DskipTests clean package
    mvn -DtagsToExclude=DOCKER test

To run integration tests specify the test suite separately e.g.:

.. sourcecode:: bash

    mvn -Dsuites=integration.CsvSelectIntegrationTest test

Quick Start - Creating scala applications with Kaspar
-----------------------------------------------------

The quick start is docker based so no external installation is required. For further examples
please see the tests in the `integration` package.

#. Start Services:

    .. sourcecode:: bash

        docker-compose up -d

#. Input some source data:

    .. sourcecode:: bash

        docker-compose exec master bash
        cd /home/ubuntu/bin
        ./setup.sh

#. Launch Spark Shell

    .. sourcecode:: bash

        docker-compose exec master bash
        cd /home/ubuntu/bin
        ./launchShell.sh

#. Set up the Kafka admin client and create a driver

    .. sourcecode:: java

        import kaspar.dataload.KasparDriverBuilder
        val clientProps = new java.util.Properties
        clientProps.setProperty("bootstrap.servers","worker1:9091")
        val kasparDriver = KasparDriverBuilder().withClientProperties(clientProps).build()

#. Set up a RowDeserializer

    .. sourcecode:: java

        import kaspar.dataload.structure.PathJsonValueRowDeserializer
        val customersDeserializer = new PathJsonValueRowDeserializer()
        val customersRowDeserializerConfig =
          """
            |{
            |  "columns" : [
            |    {
            |       "name" : "id",
            |       "type" : "Integer",
            |       "path" : "$.id"
            |     },
            |     {
            |       "name" : "name",
            |       "type" : "String",
            |       "path" : "$.name"
            |     },
            |     {
            |       "name" : "address",
            |       "type" : "String",
            |       "path" : "$.address"
            |     },
            |     {
            |       "name" : "age",
            |       "type" : "Integer",
            |       "path" : "$.age"
            |     }
            |  ]
            |}
            |""".stripMargin
        customersDeserializer.configure(customersRowDeserializerConfig)

#. Extract rows from the Customers_json topic

    .. sourcecode:: java

        val customerRawRows = kasparDriver.getRows(sc, "Customers_json", customersDeserializer)

#. Register the extracted rows as a table

    .. sourcecode:: java

        import kaspar.dataload.metadata.ColumnType
        import org.apache.spark.sql.SQLContext
        val customersColumnMappings =   Array(
          ("id",ColumnType.INTEGER),
          ("name",ColumnType.STRING),
          ("address",ColumnType.STRING),
          ("age",ColumnType.INTEGER)
        )
        val sqlContext = new SQLContext(sc)
        kasparDriver.registerTable(sqlContext, customerRawRows, "Customers", customersColumnMappings)

#. Query the table with SQL

    .. sourcecode:: java

        val sql =
          """
            | select *
            | from Customers
            |""".stripMargin
        val resultDf = sqlContext.sql(sql)
        val collected = resultDf.collect()