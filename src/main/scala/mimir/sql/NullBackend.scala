package mimir.sql;

import java.sql._

import mimir.algebra._
import net.sf.jsqlparser.statement.select.{Select, SelectBody};

class NullBackend(schema: Map[String, List[(String,Type.T)]]) extends Backend {
  def open(): Unit = {}

  def execute(sel: String): ResultSet = {
    throw new SQLException("Null backend doesn't support query execution")
  }
  def execute(sel: String, args: List[PrimitiveValue]): ResultSet = {
    throw new SQLException("Null backend doesn't support query execution")
  }

  def getTableSchema(table: String): Option[List[(String, Type.T)]] = schema.get(table)
  
  def update(stmt: String): Unit = {
    throw new SQLException("Null backend doesn't support query execution")
  }
  def update(stmt: String, args: List[PrimitiveValue]): Unit = {
    throw new SQLException("Null backend doesn't support query execution")
  }

  def getAllTables(): List[String] = schema.keys.toList

  def close() = {}

  def specializeQuery(q: Operator) = q
  def supportsInlineBestGuess() = false
  def compileForBestGuess(q: mimir.algebra.Operator, idCols: List[String]): mimir.algebra.Operator = ???

}
