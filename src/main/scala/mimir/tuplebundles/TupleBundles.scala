package mimir.tuplebundle

import java.sql.SQLException

import mimir.algebra._
import mimir.sql.sqlite._

object TupleBundles {

  val SAMPLE_COUNT = 10

  def rewrite(e: Expression){

  }

}



case class ValueBundle(t:Type.T) extends PrimitiveValue(t) {

  val v = new Array[PrimitiveValue](TupleBundles.SAMPLE_COUNT)

  def asLong: Long = throw new SQLException("Can't cast value bundle to Long")
  def asDouble: Double = throw new SQLException("Can't cast value bundle to Double")

  def asString: String = v.mkString(",")
  def payload: Object = v

}

