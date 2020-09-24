package kaspar.dataload.structure

import kafka.serializer.{Decoder, StringDecoder}
import kafka.utils.VerifiableProperties
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.utils.Utils

class CsvColumnifier(val delimiter: String = ",") extends Columnifier with Serializable {

  @transient var decoder: Decoder[String] = null

  override def toColumns(record: Record): scala.Seq[Any] = {

    if(decoder == null) {
      decoder = new StringDecoder(new VerifiableProperties)
    }

    val rawValue: String = record.offset + "," + record.timestamp + "," +
      decoder.fromBytes(Utils.readBytes(record.value))

    rawValue.split(delimiter)
  }
}
