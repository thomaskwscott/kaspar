package kaspar.dataload.structure

import com.jayway.jsonpath.JsonPath
import kafka.serializer.{Decoder, StringDecoder}
import kafka.utils.VerifiableProperties
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.metadata.ColumnType.ColumnType
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.utils.Utils

class PathJsonValueColumnifier(val fieldMappings: Seq[(String,ColumnType,String)] = Seq.empty[(String,ColumnType,String)]) extends Columnifier with Serializable {

  @transient var decoder: Decoder[String] = null

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
