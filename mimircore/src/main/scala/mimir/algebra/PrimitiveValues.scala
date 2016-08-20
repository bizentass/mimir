package mimir.algebra

import java.sql.SQLException
import mimir.algebra.Type._

/////////////// Primitive Values ///////////////

/**
 * Slightly more specific base type for constant terms.  PrimitiveValue
 * also acts as a boxing type for constants in Mimir.
 */
@SerialVersionUID(100L)
abstract class PrimitiveValue(t: Type.T) 
  extends LeafExpression 
  with Serializable
{
  def getType = t
  /**
   * Convert the current object into a long or throw a TypeException if 
   * not possible
   */
  def asLong: Long;
  /**
   * Convert the current object into a double or throw a TypeException if 
   * not possible
   */
  def asDouble: Double;
  /**
   * Convert the current object into a string or throw a TypeException if 
   * not possible.  Note the difference between this and toString.
   * asString returns the content, while toString returns a representation
   * of the primitive value itself.
   * An overt example of this is:
   *   val temp = StringPrimitive('foo')
   *   println(temp.asString)  // Returns "foo"
   *   println(temp.toString)  // Returns "'foo'"
   * Note the extra quotes.  If you ever see a problem involving strings
   * with ''too many nested quotes'', your problem is probably with asString
   */
  def asString: String;
  /**
   * return the contents of the variable as just an object.
   */
  def payload: Object;
}
/**
 * Boxed representation of a long integer
 */
@SerialVersionUID(100L)
case class IntPrimitive(v: Long) 
  extends PrimitiveValue(TInt) 
{
  override def toString() = v.toString
  def asLong: Long = v;
  def asDouble: Double = v.toDouble;
  def asString: String = v.toString;
  def payload: Object = v.asInstanceOf[Object];
}
/**
 * Boxed representation of a string
 */
@SerialVersionUID(100L)
case class StringPrimitive(v: String) 
  extends PrimitiveValue(TString)
{
  override def toString() = "'"+v.toString+"'"
  def asLong: Long = java.lang.Long.parseLong(v)
  def asDouble: Double = java.lang.Double.parseDouble(v)
  def asString: String = v;
  def payload: Object = v.asInstanceOf[Object];
}
/**
 * Boxed representation of a type object
 */
@SerialVersionUID(100L)
case class TypePrimitive(t: Type.T)
  extends PrimitiveValue(Type.TType)
{
  override def toString() = t.toString
  def asLong: Long = throw new TypeException(TType, TInt, "Cast")
  def asDouble: Double = throw new TypeException(TType, TFloat, "Cast")
  def asString: String = t.toString;
  def payload: Object = t.asInstanceOf[Object];
}
/**
 * Boxed representation of a row identifier/provenance token
 */
@SerialVersionUID(100L)
case class RowIdPrimitive(v: String)
  extends PrimitiveValue(TRowId)
{
  override def toString() = "'"+v.toString+"'"
  def asLong: Long = java.lang.Long.parseLong(v)
  def asDouble: Double = java.lang.Double.parseDouble(v)
  def asString: String = v;
  def payload: Object = v.asInstanceOf[Object];
}
/**
 * Boxed representation of a double-precision floating point number
 */
@SerialVersionUID(100L)
case class FloatPrimitive(v: Double) 
  extends PrimitiveValue(TFloat)
{
  override def toString() = v.toString
  def asLong: Long = throw new TypeException(TFloat, TInt, "Cast");
  def asDouble: Double = v
  def asString: String = v.toString;
  def payload: Object = v.asInstanceOf[Object];
}

/**
 * Boxed representation of a date
 */
@SerialVersionUID(100L)
case class DatePrimitive(y: Int, m: Int, d: Int) 
  extends PrimitiveValue(TDate)
{
  override def toString() = "DATE '"+y+"-"+m+"-"+d+"'"
  def asLong: Long = throw new TypeException(TDate, TInt, "Cast");
  def asDouble: Double = throw new TypeException(TDate, TFloat, "Cast");
  def asString: String = toString;
  def payload: Object = (y, m, d).asInstanceOf[Object];
  def compare(c: DatePrimitive): Integer = {
    if(c.y < y){ -1 }
    else if(c.y > y) { 1 }
    else if(c.m < m) { -1 }
    else if(c.m > m) { 1 }
    else if(c.d < d) { -1 }
    else if(c.d > d) { 1 }
    else { 0 }
  }
}
/**
 * Boxed representation of a boolean
 */
@SerialVersionUID(100L)
case class BoolPrimitive(v: Boolean)
  extends PrimitiveValue(TBool)
{
  override def toString() = if(v) {"TRUE"} else {"FALSE"}
  def asLong: Long = throw new TypeException(TBool, TInt, "Cast");
  def asDouble: Double = throw new TypeException(TBool, TFloat, "Cast");
  def asString: String = toString;
  def payload: Object = v.asInstanceOf[Object];
}
/**
 * Boxed representation of NULL
 */
@SerialVersionUID(100L)
case class NullPrimitive()
  extends PrimitiveValue(TAny)
{
  override def toString() = "NULL"
  def asLong: Long = throw new TypeException(TAny, TInt, "Cast Null");
  def asDouble: Double = throw new TypeException(TAny, TFloat, "Cast Null");
  def asString: String = throw new TypeException(TAny, TString, "Cast Null");
  def payload: Object = null
}
/**
 * Boxed representation of a lage blob of data
 */
@SerialVersionUID(100L)
case class BlobPrimitive(v: Array[Byte])
  extends PrimitiveValue(TBlob)
{
  override def toString() = "BLOB"
  def asLong: Long = throw new TypeException(TBlob, TInt, "Cast Null");
  def asDouble: Double = throw new TypeException(TBlob, TFloat, "Cast Null");
  def asString: String = throw new TypeException(TBlob, TString, "Cast Null");
  def payload: Object = v
}