package mimir.sql.sqlite;

import java.sql.SQLException

import mimir.algebra._
import mimir.algebra.Type._
import mimir.util._

abstract class MimirFunction extends org.sqlite.Function
{
  def value_mimir(idx: Int): PrimitiveValue =
    value_mimir(idx, TAny)

  def value_mimir(idx: Int, t:Type.T): PrimitiveValue =
  {
    if(value_type(idx) == SQLiteCompat.NULL){ NullPrimitive() }
    else { t match {
      case TInt    => IntPrimitive(value_int(idx))
      case TFloat  => FloatPrimitive(value_double(idx))
      case TDate   => JDBCUtils.convertDate(value_text(idx))
      case TString => StringPrimitive(value_text(idx))
      case TBool   => 
        BoolPrimitive(
          value_text(idx).toUpperCase() match {
            case "YES" | "TRUE"  | "1" => true
            case "NO"  | "FALSE" | "0" => false
            case _ => throw new SQLException("Invalid Bool Primitive: '"+value_text(idx)+"'")
          }
        )
      case TRowId => RowIdPrimitive(value_text(idx))
      case TType  => TypePrimitive(Type.fromString(value_text(idx)))
      case TAny   => 
        value_type(idx) match {
          case SQLiteCompat.INTEGER => IntPrimitive(value_int(idx))
          case SQLiteCompat.FLOAT   => FloatPrimitive(value_double(idx))
          case SQLiteCompat.TEXT
             | SQLiteCompat.BLOB    => StringPrimitive(value_text(idx))
        }
    }}
  }

  def return_mimir(p: PrimitiveValue): Unit =
  {
    p match {
      case IntPrimitive(i)      => result(i)
      case FloatPrimitive(f)    => result(f)
      case StringPrimitive(s)   => result(s)
      case d:DatePrimitive      => result(d.asString)
      case BoolPrimitive(true)  => result(1)
      case BoolPrimitive(false) => result(0)
      case RowIdPrimitive(r)    => result(r)
      case NullPrimitive()      => result()
    }
  }
}

abstract class SimpleMimirFunction(argTypes: List[Type.T]) extends MimirFunction
{
  def apply(args: List[PrimitiveValue]): PrimitiveValue

  override def xFunc(): Unit = {
    return_mimir(
      apply(
        argTypes.zipWithIndex.map( { case (t, i) => value_mimir(i, t) } )
      )
    )
  }
}