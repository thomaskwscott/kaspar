package kaspar.frontend.model

case class SubmitQueryResponse(queryId: String)

case class QueryStatusResponse(queryId: String, status: String)

case class QueryStatusResponses(statuses: List[QueryStatusResponse])

case class ResultSetResponse(rows: List[RowResponse])

case class RowResponse(columns: List[ColumnResponse])

case class ColumnResponse(name: String, value: String)

case class ListResultsResponse(results: List[ResultResponse])

case class ResultResponse(queryId: String)