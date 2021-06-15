package kaspar.frontend.model

import kaspar.frontend.model.ColumnType.ColumnType

case class ColumnSpecList(columnSpecs: List[ColumnSpec])

case class ColumnSpec(columnId: Int, columnName: String, tableName: String, columnType: ColumnType)

case class QueryStatusSpecList(queryStatusSpecs: List[QueryStatusSpec])

case class QueryStatusSpec(queryId: String, status: String)

case class QueryResultSpecList(queryResultSpecs: List[QueryResultSpec])

case class QueryResultSpec(queryId: String)

case class RowSpecList(rowSpecs: List[RowSpec])

case class RowSpec(columnValueSpecs: List[ColumnValueSpec])

case class ColumnValueSpec(name: String, value: String)