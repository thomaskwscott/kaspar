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

  def convert(tableSpec: TableSpec): GetTableSpecResponse = {
    GetTableSpecResponse(
      tableSpec.tableId,
      tableSpec.tableName,
      tableSpec.tableSpec
    )
  }

  def convert(tables: TableSpecList): GetTableSpecsResponseList = {
    GetTableSpecsResponseList(
      tables.tableSpecs.map(tableSpec => convert(tableSpec))
    )
  }
}