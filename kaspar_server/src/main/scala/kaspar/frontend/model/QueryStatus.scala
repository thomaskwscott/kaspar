package kaspar.frontend.model

object QueryStatus extends Enumeration {

  type QueryStatus = Value

  val RUNNING = Value("RUNNING")
  val COMPLETE = Value("COMPLETE")
  val DOES_NOT_EXIST = Value("DOES_NOT_EXIST")

}