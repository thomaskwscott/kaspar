.. _kaspar_predicates:

Kaspar Predicates
=================

Kaspar supports applying predicates at data read time at 2 levels:

  * Row predicates - a filter that is applied to each row read, unmatched rows are discarded.
  * Segment predicates - a filter that determines whether or not a whole log segment should be
    read. These typically take advantage nof indexes provided for the segment files. For instance,
    if an index indicates that the maximum value for a given field in the segment is smaller than
    the minimum value in your query then there is no need to  read this segment. These predicates
    can greatly increase performance.

Predicates are created with a PredicateGenerator (kaspar.dataload.predicate.PredicateGenerator)
offering the following interface:

.. sourcecode:: java

  def getIndexName() : String

  def getIndexFunction(columnsToIndex: Seq[(Int,ColumnType)]) : Seq[RawRow] => String

  def segmentPredicateFromJson(jsonConfig: String): (Seq[File],String, Int, String) => Boolean

  def rowPredicateFromJson(jsonConfig: String): (RawRow) => Boolean

Explanations for these can be found below:

  * getIndexName - This should be a unique name for the predicate. It is important that this is
    unique as this as it is appended to the name of any index files generated using
    getIndexFunction.
  * getIndexFunction - This returns a function that is applied to the rows in a Kafka segment file
    in order to create an index file for that segment. This file can then be used with segment
    predicates.
  * segmentPredicateFromJson - Creates a segment predicate function from a given Json config.
  * rowPredicateFromJson - Creates a row predicate function from a given Json config.

Existing PredicateGenerators
----------------------------

------------------------
MinMaxPredicateGenerator
------------------------

Creates predicates that enforce a minimum of maximum value for a row field.

Segment Predicate Example configuration:
########################################

.. sourcecode:: java

    {
      "predicateType" : "GreaterThan"
      "threshold" : 10,
      "columnIndex" : 1
    }

Row Predicate Example configuration:
####################################

Row predicates are not currently supported.

------------------------
OffsetPredicateGenerator
------------------------

Creates predicates that enforce a minimum of maximum value for a row field.

Segment Predicate Example configuration:
########################################

.. sourcecode:: java

    {
      "predicateType": "GreaterThan"
      "partitionThresholds" : [
        { "partition": 0, "threshold": 10 },
        { "partition": 1, "threshold": 20 }
      ]
    }

Row Predicate Example configuration:
####################################

.. sourcecode:: java

    {
      "predicateType": "GreaterThan"
      "partitionThresholds" : [
        { "partition": 0, "threshold": 10 },
        { "partition": 1, "threshold": 20 }
      ]
    }