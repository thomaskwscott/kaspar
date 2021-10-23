.. _kaspar_server_config:

Kaspar Server Configuration Options
-----------------------------------

Rest
^^^^

``rest.port``
  The port on which the run the Kaspar server

  * Type: int
  * Importance: high

Metastore
^^^^^^^^^

``metastore.jdbc.url``
  The url to use to connect to the metastore backing database.

  * Type: string
  * Default: null
  * Importance: high

``metastore.jdbc.class``
  The jdbc driver class to use

  * Type: string
  * Default: null
  * Importance: high

``metastore.jdbc.pool.min.size``
  The minimum size of the backing DB connection pool

  * Type: int
  * Default: 1
  * Importance: low

``metastore.jdbc.pool.max.size``
  The maximum size of the backing DB connection pool

  * Type: int
  * Default: 5
  * Importance: low

Kafka
^^^^^

Kafka properties configure the adminclient used to determine segment file locations etc. All properties in this config are submitted so these can be provided with no prefix.

``bootstrap.servers``
  List of endpoints to bootstrap to

  * Type: string
  * Default: null
  * Importance: high

Kaspar
^^^^^^

``runner.pool.size``
  The number of simultaneous queries that can be handled by this server.

  * Type: int
  * Default: 1
  * Importance: low
