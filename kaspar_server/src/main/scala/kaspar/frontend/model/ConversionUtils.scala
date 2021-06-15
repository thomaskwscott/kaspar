package kaspar.frontend.model

object ConversionUtils {

  def convert(queryStatuses: QueryStatusSpecList): GetQueryStatusResponseList = {
    GetQueryStatusResponseList(
      queryStatuses.queryStatusSpecs.map(status => GetQueryStatusResponse(
        status.queryId,
        status.status
      ))
    )
  }

  def convert(resultRowSet: RowSpecList): GetRowResponseList = {
    GetRowResponseList(
      resultRowSet.rowSpecs.map(rows => GetRowResponse(rows.columnValueSpecs.map(column => GetColumnResponse(column.name,column.value))))
    )
  }

  def convert(queryResults: QueryResultSpecList): GetResultResponseList = {
    GetResultResponseList(
      queryResults.queryResultSpecs.map(result => GetResultResponse(result.queryId))
    )
  }

  def convert(columnSpec: ColumnSpec): GetColumnSpecResponse = {
    GetColumnSpecResponse(
      columnSpec.columnId,
      columnSpec.columnName,
      columnSpec.tableName,
      columnSpec.columnType.toString
    )
  }

  def convert(columns: ColumnSpecList): GetColumnSpecsResponseList = {
    GetColumnSpecsResponseList(
      columns.columnSpecs.map(columnSpec => convert(columnSpec))
    )
  }
}