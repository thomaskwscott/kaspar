package kaspar.dataload.structure

import org.apache.kafka.common.record.Record


trait Columnifier {
  def toColumns(raw: Record):Seq[Any]
}
