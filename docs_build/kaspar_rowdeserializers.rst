.. _kaspar_rowdeserializers:

Kaspar Row Deserializers
========================

Row Deserializers perform the conversion of Kafka messages from byte arrays to Row Structures.
These all implement the RowDeserializer trait kaspar.dataload.structure.RowDeserializer and
provide:

.. sourcecode:: java

  def configure(configuration: String): Unit

  def toColumns(partition: Int, raw: Record):Seq[Any]

To use deserializers, developers should create an instance and configure it with a
configuration string (usually json). Kaspar will then use this internally to do the conversion.

Existing RowDeserializers
-------------------------

------------------
CsvRowDeserializer
------------------

Converts character separated (csv) data into rows.

Example configuration:
######################

.. sourcecode:: java

    {
      "delimiter" : ","
    }

Example input:
##############

.. sourcecode:: java

    val1,val2

Becomes:
########

.. sourcecode:: java

    ["val1","val2"]

---------------------------
JsonKeyValueRowDeserializer
---------------------------

Converts Json data from both the message key and value fields into row data.

Example configuration:
######################

.. sourcecode:: java

    {
      "columns" : [
        {
          "name": "col1",
          "type": "Integer",
          "isKey": true
        },
        {
          "name": "col2",
          "type": "Integer",
          "isKey": false
        }
    }

Example input:
##############

Key:

.. sourcecode:: java

    { "col1": 1 }

Value:

.. sourcecode:: java

    { "col2" : 2}

Becomes:
########
.. sourcecode:: java

    [1,2]

------------------------
JsonValueRowDeserializer
------------------------

Converts Json data from the message value field into row data.

Example configuration:
######################

.. sourcecode:: java

    {
      "columns" : [
        {
          "name": "col1",
          "type": "Integer",
        },
        {
          "name": "col2",
          "type": "Integer",
        }
    }

Example input:
##############

.. sourcecode:: java

    {
      "col1": 1,
      "col2": 2
    }

Becomes:
########
.. sourcecode:: java

    [1,2]

----------------------------
PathJsonValueRowDeserializer
----------------------------

Converts Json data from the message value field into row data, using JsonPath to determine where
to fetch the column value from.

Example configuration:
######################

.. sourcecode:: java

    {
      "columns" : [
        {
          "name": "col1",
          "type": "Integer",
          "path": "$.col1"
        },
        {
          "name": "col2",
          "type": "Integer",
          "path": "$.someNode.col2"
        }
    }

Example input:
##############

.. sourcecode:: java

    {
      "col1": 1,
      "someNode" : {
        "col2": 2
      }
    }

Becomes:
########
.. sourcecode:: java

    [1,2]