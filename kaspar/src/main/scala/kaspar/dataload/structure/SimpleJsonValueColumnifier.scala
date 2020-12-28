package kaspar.dataload.structure

import kafka.serializer.{Decoder, StringDecoder}
import kafka.utils.VerifiableProperties
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.metadata.ColumnType.ColumnType
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.utils.Utils

import org.json4s._
import org.json4s.jackson.JsonMethods._


class SimpleJsonValueColumnifier(val fieldMappings: Seq[(String,ColumnType)] = Seq.empty[(String,ColumnType)]) extends Columnifier with Serializable {

  @transient var decoder: Decoder[String] = null

  override def toColumns(partition: Int, record: Record): scala.Seq[Any] = {

    if(decoder == null) {
      decoder = new StringDecoder(new VerifiableProperties)
    }

    val fields = parse(decoder.fromBytes(Utils.readBytes(record.value))).asInstanceOf[JObject]


    Array[Any](record.offset,partition, record.timestamp) ++ fieldMappings.map {i => {
        implicit val formats = DefaultFormats
        val fieldVal = (fields \ i._1)
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
