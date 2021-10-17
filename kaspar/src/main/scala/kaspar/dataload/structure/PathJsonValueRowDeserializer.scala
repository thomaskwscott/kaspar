package kaspar.dataload.structure

import com.jayway.jsonpath.JsonPath
import kafka.serializer.{Decoder, StringDecoder}
import kafka.utils.VerifiableProperties
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.metadata.ColumnType.ColumnType
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable.ListBuffer

class PathJsonValueRowDeserializer() extends RowDeserializer with Serializable {

  var fieldMappings : ListBuffer[(String, ColumnType, String)] = ListBuffer()

  @transient var decoder: Decoder[String] = null

  override def configure(configuration: String) = {
    val configObj = JsonPath.parse(configuration)
    // get columns from config
    /*
    Example json:
    {
      "columns" : [
        {
          "name": "someCol",
          "type": "LONG",
          "path": "$.someCol"
        },
        {
          "name": "someCol2",
          "type": "LONG",
          "path": "$.someCol"
        }
      ]
    }
     */
    val columnCount = configObj.read[Int]("$.columns.length()")
    for( columnIndex <- 0 to columnCount-1)  {
      val columnName = configObj.read[String]("$.columns[" + columnIndex + "].name")
      val columnType = configObj.read[String]("$.columns[" + columnIndex + "].type")
      val columnPath = configObj.read[String]("$.columns[" + columnIndex + "].path")
      fieldMappings += ((columnName, ColumnType.withName(columnType), columnPath))
    }
  }

  override def toColumns(partition: Int, record: Record): scala.Seq[Any] = {

    if(decoder == null) {
      decoder = new StringDecoder(new VerifiableProperties)
    }

    val fields = JsonPath.parse(decoder.fromBytes(Utils.readBytes(record.value)))

    Array[Any](record.offset,partition, record.timestamp) ++ fieldMappings.map {i => {
        i._2 match {
          case ColumnType.INTEGER => fields.read[Int](i._3)
          case ColumnType.LONG => fields.read[Long](i._3)
          case ColumnType.DOUBLE => fields.read[Double](i._3)
          case default => fields.read[String](i._3)
        }
      }
    }
  }
}
