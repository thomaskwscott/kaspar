.. _kaspar:

Kaspar
======

Contents:

.. toctree::
   :maxdepth: 3

   kaspar_core
   kaspar_server
   changelog

What is Kaspar
##############

Kaspar is a batch based SQL engine running on top of Apache Kafka. It co-locates Spark workers
with Kafka brokers in order to give access to the underlying log segments that feed Kafka and
query them in a batch oriented way rather than the traditional produce/consume.

.. code-block:: java

    +--------+  +--------+  +--------+  +--------+
    | Kafka  |  | Kafka  |  | Kafka  |  | Spark  |
    |        |  |        |  |        |  | Master |
    | Spark  |  | Spark  |  | Spark  |  |        |
    | Worker |  | Worker |  | Worker |  |        |
    +--------+  +--------+  +--------+  +--------+

Start with Why
##############

Why Kaspar?

  * Organisations are increasingly moving towards centralised Kafka deployments. This means that
    cross domain data is already being landed into Kafka - Kafka is your data lake.
  * Organisations are increasingly using high or infinite retention configurations in Kafka. This
    means that historical data for model training etc. is available in Kafka but inconvienient to
    access.
  * Using Kaspar avoids the traditional extra copies of data in your analytical system
    (Hadoop etc.) by querying the data where it is created in Kafka
  * Kaspar can handle much larger data sets than the current SQL on Kafka solution (KSQLDB).

Kaspar Components
#################

Kaspar is made up of 2 components:

#. Kaspar Core - The core library for use in Scala applications and Spark Shell
#. Kaspar Server - An application that provides a REST interface for running SQL with Kaspar