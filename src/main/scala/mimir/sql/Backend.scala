package mimir.sql;

import java.sql._

import mimir.algebra._
import mimir.util.JDBCUtils
import net.sf.jsqlparser.statement.select.{Select, SelectBody};

abstract class Backend {
  def open(): Unit

  def execute(sel: String): ResultSet
  def execute(sel: String, args: List[PrimitiveValue]): ResultSet
  def execute(sel: Select): ResultSet = 
    execute(sel.toString());
  def execute(sel: Select, args: List[PrimitiveValue]): ResultSet = 
    execute(sel.toString(), args);
  def execute(selB: SelectBody, args: List[PrimitiveValue]): ResultSet = {
    val sel = new Select();
    sel.setSelectBody(selB);
    if(args == Nil){
      return execute(sel);
    } else {
      return execute(sel, args);
    }
  }
  def execute(selB: SelectBody): ResultSet = 
    execute(selB, Nil)

  def resultRows(sel: String) = 
    JDBCUtils.extractAllRows(execute(sel))
  def resultRows(sel: String, args: List[PrimitiveValue]) =
    JDBCUtils.extractAllRows(execute(sel, args))
  def resultRows(sel: Select) =
    JDBCUtils.extractAllRows(execute(sel))
  def resultRows(sel: Select, args: List[PrimitiveValue]) =
    JDBCUtils.extractAllRows(execute(sel, args))
  def resultRows(sel: SelectBody) =
    JDBCUtils.extractAllRows(execute(sel))
  def resultRows(sel: SelectBody, args: List[PrimitiveValue]) =
    JDBCUtils.extractAllRows(execute(sel, args))
  
  def getTableSchema(table: String): Option[List[(String, Type.T)]]
  def getTableOperator(table: String): Operator =
    getTableOperator(table, List[(String,Expression,Type.T)]())
  def getTableOperator(table: String, metadata: List[(String, Expression, Type.T)]):
    Operator =
  {
    Table(
      table, 
      getTableSchema(table) match {
        case Some(x) => x
        case None => throw new SQLException("Table does not exist in db!")
      },
      metadata
    )
  }
  
  def update(stmt: String): Unit
  def update(stmt: List[String]): Unit =
    stmt.foreach(update(_))
  def update(stmt: String, args: List[PrimitiveValue]): Unit
  def update(stmt: Statement): Unit =
    update(stmt.toString)
  def update(stmt: Statement, args: List[PrimitiveValue]): Unit =
    update(stmt.toString, args)

  def getAllTables(): List[String]

  def close()

  def specializeQuery(q: Operator): Operator
  def supportsInlineBestGuess(): Boolean
  def compileForBestGuess(q: Operator): Operator

}
