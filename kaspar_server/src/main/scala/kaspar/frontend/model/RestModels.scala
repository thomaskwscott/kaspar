package kaspar.frontend.model

case class PostQueryResponse(queryId: String)

case class GetQueryStatusResponse(queryId: String, status: String)

case class GetQueryStatusResponseList(getQueryStatusResponses: List[GetQueryStatusResponse])

case class GetRowResponseList(getRowResponses: List[GetRowResponse])

case class GetRowResponse(getColumnResponses: List[GetColumnResponse])

case class GetColumnResponse(name: String, value: String)

case class GetResultResponseList(getResultResponses: List[GetResultResponse])

case class GetResultResponse(queryId: String)

case class PostColumnSpecResponse(columnId: Int)

case class GetColumnSpecResponse(columnId: Int, columnName: String, tableName: String, columnType: String)

case class GetColumnSpecsResponseList(getColumnSpecResponses: List[GetColumnSpecResponse])

case class PostColumnSpecRequest(columnName: String, tableName: String, columnType: String)