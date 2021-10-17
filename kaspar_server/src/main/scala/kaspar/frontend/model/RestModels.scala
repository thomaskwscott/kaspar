package kaspar.frontend.model

case class PostQueryResponse(queryId: String)

case class GetQueryStatusResponse(queryId: String, status: String)

case class GetQueryStatusResponseList(getQueryStatusResponses: List[GetQueryStatusResponse])

case class GetRowResponseList(getRowResponses: List[GetRowResponse])

case class GetRowResponse(getColumnResponses: List[GetColumnResponse])

case class GetColumnResponse(name: String, value: String)

case class GetResultResponseList(getResultResponses: List[GetResultResponse])

case class GetResultResponse(queryId: String)

case class PostTableSpecResponse(tableId: Int)

case class GetTableSpecResponse(tableId: Int, tableName: String, tableSpec: String)

case class GetTableSpecsResponseList(getTableSpecResponses: List[GetTableSpecResponse])

case class PostTableSpecRequest(tableName: String, tableSpec: String)

