package kaspar.frontend.model

import kaspar.frontend.model.ColumnType.ColumnType

case class TableSpecList(tableSpecs: List[TableSpec])

case class TableSpec(tableId: Int, tableName: String, tableSpec: String)

case class QueryStatusSpecList(queryStatusSpecs: List[QueryStatusSpec])

case class QueryStatusSpec(queryId: String, status: String)

case class QueryResultSpecList(queryResultSpecs: List[QueryResultSpec])

case class QueryResultSpec(queryId: String)

case class RowSpecList(rowSpecs: List[RowSpec])

case class RowSpec(columnValueSpecs: List[ColumnValueSpec])

case class ColumnValueSpec(name: String, value: String)