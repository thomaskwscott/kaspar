package kaspar.dataload.metadata

case class TaskAssignment(partitionId:Int,
                          locations: Seq[Location]
                         )
case class Location(host: String,
                    brokerId: Int
                   )