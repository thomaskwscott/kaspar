package kaspar.dataload.structure

import org.apache.kafka.common.record.Record

// columnifiers shgould have empty constructors and use configure
trait RowDeserializer {

  /* the configure method takes a json/properties string used for
    configuring the columnifier. Deserialization here is expensive
    but paid only once per query per worker and it greatly simplifies
    other interfaces

    for deserializers that require mappings they can expect a "columns" field. e.g.
    {
      "columns" : [
         {
           "name" : "someCol",
           "type" : "Integer"
           "isKey" : true
          }
        ]
     }
   */
  def configure(configuration: String): Unit

  def toColumns(partition: Int, raw: Record):Seq[Any]
}
