package kaspar.dataload.structure

import kaspar.dataload.metadata.ColumnType
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.record.{Record, TimestampType}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.scalatest.flatspec.AnyFlatSpec

import java.io.ByteArrayInputStream
import java.lang
import java.nio.ByteBuffer

class JsonValueRowDeserializerTest extends AnyFlatSpec {

  val COLUMN_NAME = "col1"
  val COLUMN_TYPE = ColumnType.INTEGER
  val CONFIGURATION =
    s"""
      |{
      |  "columns" : [
      |    {
      |       "name" : "$COLUMN_NAME",
      |       "type" : "$COLUMN_TYPE"
      |     }
      |  ]
      |}
      |""".stripMargin

  val RECORD_VALUE =
    s"""
      |{
      |  "$COLUMN_NAME" : 123
      |}
      |""".stripMargin

  "A JsonValueRowDeserializer" should "load fieldMappings from configuration" in {
    val deserializer = new JsonValueRowDeserializer()
    deserializer.configure(CONFIGURATION)

    val columnVals = deserializer.toColumns(0, createRecord)
    assert(columnVals(3)==123)
  }

  "A JsonValueRowDeserializer" should "serialize fieldMappings" in {

    val deserializer = new JsonValueRowDeserializer()
    deserializer.configure(CONFIGURATION)

    val conf = new SparkConf()
    val out = new ByteArrayOutputStream()
    val ser = new KryoSerializer(conf).newInstance() // Here I test using KryoSerializer, you can use JavaSerializer too
    val serOut = ser.serializeStream(out)

    serOut.writeObject(deserializer)
    serOut.flush()

    val bytes = out.toByteArray

    val serIn = ser.deserializeStream(new ByteArrayInputStream(bytes))
    val deserialized = serIn.readObject().asInstanceOf[JsonValueRowDeserializer]

    assert(deserialized.fieldMappings.size == 1)
    assert(deserialized.fieldMappings(0)._1 == COLUMN_NAME)
    assert(deserialized.fieldMappings(0)._2 == COLUMN_TYPE)
  }

  private def createRecord: Record = {
    new Record {
      override def offset(): Long = 0

      override def sequence(): Int = 0

      override def sizeInBytes(): Int = 10

      override def timestamp(): Long = 10

      override def checksumOrNull(): lang.Long = 0

      override def isValid: Boolean = true

      override def ensureValid(): Unit = ()

      override def keySize(): Int = 0

      override def hasKey: Boolean = false

      override def key(): ByteBuffer = ByteBuffer.wrap(Array[Byte]())

      override def valueSize(): Int = 10

      override def hasValue: Boolean = true

      override def value(): ByteBuffer = ByteBuffer.wrap(RECORD_VALUE.getBytes)

      override def hasMagic(b: Byte): Boolean = false

      override def isCompressed: Boolean = false

      override def hasTimestampType(timestampType: TimestampType): Boolean = false

      override def headers(): Array[Header] = Array[Header]()
    }
  }
}
