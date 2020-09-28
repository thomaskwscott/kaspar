package kaspar.dataload.structure

import kafka.serializer.{Decoder, StringDecoder}
import kafka.utils.VerifiableProperties
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.metadata.ColumnType.ColumnType
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.utils.Utils

import scala.util.parsing.json.JSON

class JsonColumnifier(val fieldMappings: Seq[(String,ColumnType)] = Seq.empty[(String,ColumnType)]) extends Columnifier with Serializable {

  @transient var decoder: Decoder[String] = null

  override def toColumns(partition: Int, record: Record): scala.Seq[Any] = {

    if(decoder == null) {
      decoder = new StringDecoder(new VerifiableProperties)
    }

    val fields = JSON.parseFull(decoder.fromBytes(Utils.readBytes(record.value))).get.asInstanceOf[Map[String, Any]]

    Array(record.offset,partition, record.timestamp) ++ fieldMappings.map {i => {
        i._2 match {
          case ColumnType.INTEGER => fields(i._1).asInstanceOf[Double].toInt
          case ColumnType.LONG => fields(i._1).asInstanceOf[Double].toLong
          case ColumnType.DOUBLE => fields(i._1)
          case ColumnType.STRING => fields(i._1)
          case default => fields(i._1).toString
        }
      }
    }
  }
}
