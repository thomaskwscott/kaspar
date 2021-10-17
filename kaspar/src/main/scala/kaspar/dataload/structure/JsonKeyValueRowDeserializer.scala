package kaspar.dataload.structure

import com.jayway.jsonpath.JsonPath
import kafka.serializer.{Decoder, StringDecoder}
import kafka.utils.VerifiableProperties
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.metadata.ColumnType.ColumnType
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.utils.Utils
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ListBuffer

class JsonKeyValueRowDeserializer() extends RowDeserializer with Serializable {

  var fieldMappings : ListBuffer[(String,ColumnType, Boolean)] = ListBuffer()

  @transient var decoder: Decoder[String] = null

  override def configure(configuration: String) = {
    val configObj = JsonPath.parse(configuration)
    // get columns from config
    val columnCount = configObj.read[Int]("$.columns.length()")
    for( columnIndex <- 0 to columnCount-1)  {
      val columnName = configObj.read[String]("$.columns[" + columnIndex + "].name")
      val columnType = configObj.read[String]("$.columns[" + columnIndex + "].type")
      val isKey = configObj.read[Boolean]("$.columns[" + columnIndex + "].isKey")
      fieldMappings += ((columnName, ColumnType.withName(columnType), isKey))
    }
  }

  override def toColumns(partition: Int, record: Record): scala.Seq[Any] = {

    if(decoder == null) {
      decoder = new StringDecoder(new VerifiableProperties)
    }

    val keyFields = if(record.hasKey) {
      parse(decoder.fromBytes(Utils.readBytes(record.key()))).asInstanceOf[JObject]
    } else {
      null
    }
    val valueFields = parse(decoder.fromBytes(Utils.readBytes(record.value))).asInstanceOf[JObject]

    Array[Any](record.offset,partition, record.timestamp) ++ fieldMappings.map {i => {
        implicit val formats = DefaultFormats

        val fieldVal = if (i._3) (keyFields \ i._1) else (valueFields \ i._1)
        i._2 match {
          case ColumnType.INTEGER => fieldVal.extract[Int]
          case ColumnType.LONG => fieldVal.extract[Long]
          case ColumnType.DOUBLE => fieldVal.extract[Double]
          case default => fieldVal.extract[String]
        }
      }
    }
  }
}
