package statement

import net.sf.jsqlparser.expression
import net.sf.jsqlparser.expression.operators.arithmetic.{Addition, BitwiseAnd, BitwiseLeftShift, BitwiseOr, BitwiseRightShift, BitwiseXor, Concat, Division, IntegerDivision, Modulo, Multiplication, Subtraction}
import net.sf.jsqlparser.expression.operators.conditional.{AndExpression, OrExpression}
import net.sf.jsqlparser.expression.operators.relational.{Between, EqualsTo, ExistsExpression, FullTextSearch, GreaterThan, GreaterThanEquals, InExpression, IsBooleanExpression, IsNullExpression, ItemsListVisitor, JsonOperator, LikeExpression, Matches, MinorThan, MinorThanEquals, NotEqualsTo, RegExpMatchOperator, RegExpMySQLOperator, SimilarToExpression}
import net.sf.jsqlparser.expression.{AllComparisonExpression, AnalyticExpression, AnyComparisonExpression, ArrayExpression, CaseExpression, CastExpression, CollateExpression, DateTimeLiteralExpression, DateValue, DoubleValue, ExpressionVisitor, ExtractExpression, HexValue, IntervalExpression, JdbcNamedParameter, JdbcParameter, JsonExpression, KeepExpression, LongValue, MySQLGroupConcat, NextValExpression, NotExpression, NullValue, NumericBind, OracleHierarchicalExpression, OracleHint, Parenthesis, RowConstructor, SignedExpression, StringValue, TimeKeyExpression, TimeValue, TimestampValue, UserVariable, ValueListExpression, VariableAssignment, WhenClause, XMLSerializeExpr}
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.{Column, Table}
import net.sf.jsqlparser.statement.select.{FromItemVisitor, Join, LateralSubSelect, ParenthesisFromItem, PlainSelect, Select, SelectVisitor, SetOperationList, SubJoin, SubSelect, TableFunction, ValuesList, WithItem}
import net.sf.jsqlparser.statement.values.ValuesStatement
import statement.ExpressionType.ExpressionType

import scala.collection.mutable.ListBuffer

class StatementAnalyzer (val statement: String)
     extends SelectVisitor
     with ExpressionVisitor
     with FromItemVisitor {

  val parsedStatement = CCJSqlParserUtil.parse(statement).asInstanceOf[Select]
  val columnConstantsAccumulator = new ColumnConstantsAccumulator()
  var tables = ListBuffer[TableNameAlias]()

  def getColumnConstants(): Seq[ColumnConstant]= {
    columnConstantsAccumulator.reset()
    parsedStatement.getSelectBody.accept(this)
    columnConstantsAccumulator.getColumnConstants()
  }

  def getTableList():  Seq[TableNameAlias] = {
    tables.clear()
    parsedStatement.getSelectBody.accept(this)
    tables
  }

  override def visit(column: Column): Unit = {
    columnConstantsAccumulator.addExpression(ExpressionType.Column,column.getFullyQualifiedName)
  }

  override def visit(stringValue: StringValue): Unit = {
    columnConstantsAccumulator.addExpression(ExpressionType.Constant, stringValue.getValue)
  }

  override def visit(andExpression: AndExpression): Unit = {
    andExpression.getLeftExpression.accept(this)
    andExpression.getRightExpression.accept(this)
  }

  override def visit(equalsTo: EqualsTo): Unit = {
    equalsTo.getLeftExpression.accept(this)
    equalsTo.getRightExpression.accept(this)
  }

  override def visit(inExpression: InExpression): Unit = {
    if (inExpression.getLeftExpression != null) {
      inExpression.getLeftExpression.accept(this)
    }
    if (inExpression.getRightExpression != null) {
      inExpression.getRightExpression.accept(this)
    }
  }

  override def visit(plainSelect: PlainSelect): Unit = {
    plainSelect.getFromItem.accept(this)

    if (plainSelect.getJoins != null) {
      val joinsIt = plainSelect.getJoins.iterator
      while ( {
        joinsIt.hasNext
      }) {
        val join = joinsIt.next.asInstanceOf[Join]
        join.getRightItem.accept(this)
      }
    }

    if (plainSelect.getWhere != null) plainSelect.getWhere.accept(this)
  }

  override def visit(table: Table): Unit = {
    tables += TableNameAlias(table.getName, table.getAlias.getName)

  }

  override def visit(subJoin: SubJoin): Unit = {
    subJoin.getLeft.accept(this)
    subJoin.getJoinList.forEach( join =>
      join.getRightItem.accept(this)
    )
  }

  override def visit(bitwiseRightShift: BitwiseRightShift): Unit = {}

  override def visit(bitwiseLeftShift: BitwiseLeftShift): Unit = {}

  override def visit(nullValue: NullValue): Unit = {}

  override def visit(function: expression.Function): Unit = {}

  override def visit(signedExpression: SignedExpression): Unit = {}

  override def visit(jdbcParameter: JdbcParameter): Unit = {}

  override def visit(jdbcNamedParameter: JdbcNamedParameter): Unit = {}

  override def visit(doubleValue: DoubleValue): Unit = {}

  override def visit(longValue: LongValue): Unit = {}

  override def visit(hexValue: HexValue): Unit = {}

  override def visit(dateValue: DateValue): Unit = {}

  override def visit(timeValue: TimeValue): Unit = {}

  override def visit(timestampValue: TimestampValue): Unit = {}

  override def visit(parenthesis: Parenthesis): Unit = {}

  override def visit(addition: Addition): Unit = {}

  override def visit(division: Division): Unit = {}

  override def visit(integerDivision: IntegerDivision): Unit = {}

  override def visit(multiplication: Multiplication): Unit = {}

  override def visit(subtraction: Subtraction): Unit = {}

  override def visit(orExpression: OrExpression): Unit = {}

  override def visit(between: Between): Unit = {}

  override def visit(greaterThan: GreaterThan): Unit = {}

  override def visit(greaterThanEquals: GreaterThanEquals): Unit = {}

  override def visit(fullTextSearch: FullTextSearch): Unit = {}

  override def visit(isNullExpression: IsNullExpression): Unit = {}

  override def visit(isBooleanExpression: IsBooleanExpression): Unit = {}

  override def visit(likeExpression: LikeExpression): Unit = {}

  override def visit(minorThan: MinorThan): Unit = {}

  override def visit(minorThanEquals: MinorThanEquals): Unit = {}

  override def visit(notEqualsTo: NotEqualsTo): Unit = {}

  override def visit(subSelect: SubSelect): Unit = {}

  override def visit(caseExpression: CaseExpression): Unit = {}

  override def visit(whenClause: WhenClause): Unit = {}

  override def visit(existsExpression: ExistsExpression): Unit = {}

  override def visit(allComparisonExpression: AllComparisonExpression): Unit = {}

  override def visit(anyComparisonExpression: AnyComparisonExpression): Unit = {}

  override def visit(concat: Concat): Unit = {}

  override def visit(matches: Matches): Unit = {}

  override def visit(bitwiseAnd: BitwiseAnd): Unit = {}

  override def visit(bitwiseOr: BitwiseOr): Unit = {}

  override def visit(bitwiseXor: BitwiseXor): Unit = {}

  override def visit(castExpression: CastExpression): Unit = {}

  override def visit(modulo: Modulo): Unit = {}

  override def visit(analyticExpression: AnalyticExpression): Unit = {}

  override def visit(extractExpression: ExtractExpression): Unit = {}

  override def visit(intervalExpression: IntervalExpression): Unit = {}

  override def visit(oracleHierarchicalExpression: OracleHierarchicalExpression): Unit = {}

  override def visit(regExpMatchOperator: RegExpMatchOperator): Unit = {}

  override def visit(jsonExpression: JsonExpression): Unit = {}

  override def visit(jsonOperator: JsonOperator): Unit = {}

  override def visit(regExpMySQLOperator: RegExpMySQLOperator): Unit = {}

  override def visit(userVariable: UserVariable): Unit = {}

  override def visit(numericBind: NumericBind): Unit = {}

  override def visit(keepExpression: KeepExpression): Unit = {}

  override def visit(mySQLGroupConcat: MySQLGroupConcat): Unit = {}

  override def visit(valueListExpression: ValueListExpression): Unit = {}

  override def visit(rowConstructor: RowConstructor): Unit = {}

  override def visit(oracleHint: OracleHint): Unit = {}

  override def visit(timeKeyExpression: TimeKeyExpression): Unit = {}

  override def visit(dateTimeLiteralExpression: DateTimeLiteralExpression): Unit = {}

  override def visit(notExpression: NotExpression): Unit = {}

  override def visit(nextValExpression: NextValExpression): Unit = {}

  override def visit(collateExpression: CollateExpression): Unit = {}

  override def visit(similarToExpression: SimilarToExpression): Unit = {}

  override def visit(arrayExpression: ArrayExpression): Unit = {}

  override def visit(variableAssignment: VariableAssignment): Unit = {}

  override def visit(xmlSerializeExpr: XMLSerializeExpr): Unit = {}

  override def visit(setOperationList: SetOperationList): Unit = {}

  override def visit(withItem: WithItem): Unit = {}

  override def visit(valuesStatement: ValuesStatement): Unit = {}

  override def visit(lateralSubSelect: LateralSubSelect): Unit = {}

  override def visit(valuesList: ValuesList): Unit = {}

  override def visit(tableFunction: TableFunction): Unit = {}

  override def visit(parenthesisFromItem: ParenthesisFromItem): Unit = {}
}

class ColumnConstantsAccumulator {
  var columnConstants  = ListBuffer[ColumnConstant]()
  var left  = ColumnOrConstant()

  def addExpression(expressionType: ExpressionType,value: String) =  {
    if ( left.isEmpty ) {
      left = ColumnOrConstant(expressionType,value)
    } else {
      // incoming expression is right side
      if (expressionType == ExpressionType.Column && left.expressionType == ExpressionType.Constant)
      {
        columnConstants+= ColumnConstant(value,left.value)
      }
      if (expressionType == ExpressionType.Constant && left.expressionType == ExpressionType.Column) {
        columnConstants += ColumnConstant(left.value, value)
      }
      left = ColumnOrConstant()
    }
  }

  def reset(): Unit ={
    columnConstants.clear();
  }

  def getColumnConstants(): Seq[ColumnConstant] = {
    columnConstants
  }

}

case class TableNameAlias(name: String, alias: String)

case class ColumnOrConstant(expressionType: ExpressionType = null, value: String = null) {
  def isEmpty: Boolean = {value == null}
}

case class ColumnConstant(columnName: String, value: String)

object ExpressionType extends Enumeration {
  type ExpressionType = Value
  val Column, Constant = Value
}

object StatementAnalyzer {
  def main(args: Array[String]): Unit = {
    val statement = "SELECT * FROM TEST_TABLE t, TAB2 t2 WHERE t.COL1 = 'A' AND t2.COL2 = 'B' AND t.COL3 = t2.COL4"
    val analyzer = new StatementAnalyzer(statement)
    analyzer.getColumnConstants().foreach(entry => println(entry.columnName + " = " + entry.value))
    analyzer.getTableList().foreach(table =>  println(table.name + ":" + table.alias))
  }
}
