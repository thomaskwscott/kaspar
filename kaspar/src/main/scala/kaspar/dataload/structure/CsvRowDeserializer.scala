package kaspar.dataload.structure

import kafka.serializer.{Decoder, StringDecoder}
import kafka.utils.VerifiableProperties
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.utils.Utils
import com.jayway.jsonpath.JsonPath

class CsvRowDeserializer() extends RowDeserializer with Serializable {

  val DELIMITER_PATH = "$.delimiter"

  var delimiter: String = ","

  @transient var decoder: Decoder[String] = null

  override def configure(configuration: String) = {
    val configObj = JsonPath.parse(configuration)
    delimiter = configObj.read[String](DELIMITER_PATH)
  }

  override def toColumns(partition: Int, record: Record): scala.Seq[Any] = {

    if(decoder == null) {
      decoder = new StringDecoder(new VerifiableProperties)
    }

    val rawValue: String = record.offset + delimiter + partition + delimiter + record.timestamp + delimiter +
      decoder.fromBytes(Utils.readBytes(record.value))

    rawValue.split(delimiter)
  }
}
