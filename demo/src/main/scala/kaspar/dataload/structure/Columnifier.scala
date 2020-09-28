package kaspar.dataload.structure

import org.apache.kafka.common.record.Record


trait Columnifier {
  def toColumns(partition: Int, raw: Record):Seq[Any]
}
