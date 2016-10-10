package mimir.tuplebundle

import java.sql.SQLException

import mimir.algebra._
import mimir.sql.sqlite._

object TupleBundles {

  val SAMPLE_COUNT = 10
  val collections.mutable.map 

  
}

class TupleBundleRewriter(db: Database, conn: java.sql.Connection) {

  var tupleBundleIdx = 0;

  def nextFname(): String = 
  {
    tupleBundleIdx += 1;
    "__MIMIR_SQLITE_TUPLEBUNDLE_"+tupleBundleIdx
  }

  def rewrite(expr: Expression, schema:Map[String,Type.T]): (Expression, Boolean) =
  {
    val argVars = ExpressionUtils.getColumns(arg.expression)
    // Check to see if the expression needs to be rewritten as a tuple
    // bundle.  This is the case if...
    //  - The expression references an input that is itself a tuple bundle
    //  - The expression references a VGTerm
    if(  !((argVars & childBundles).empty) || 
         (CTables.isProbabilistic(arg.expression)) ){
      val schema = argVars.toList()
      val fname = nextFname()
      FunctionRegistry.registerFunction(
        fname,
        (args: List[PrimitiveValue]) => Eval.eval(expr, schema.zip(args).toMap),
        (args: List[Type.T]) => ExprChecker(schema.zip(args).toMap).typecheck(expr)
      )
      val eval = new TupleBundleEval(schema, expr)
      org.sqlite.Function.create(conn, fname, eval)
      (Function(fname, schema.map(Var(_))), true)
    } else {
      (expr, false)
    }

  }


  def rewrite(oper: Operator): (Operator, Set[String], List[String]) =
  {
    oper match {
      case Project(args, child) => {
        val (newChild, childBundles, tuplePresence) = rewrite(child)
        val (newArgs, newBundles) = args.map( (arg) => {
            val (newExpr, needsBundle) = rewrite(arg.expression)
            (
              ProjectArg(arg.name, newExpr), 
              if(needsBundle){ Some(arg.name) } else { None }
            )
          }).unzip
        (
          Project(
            newArgs++tuplePresence.map(x=>ProjectArg(x,Var(x))), 
            newChild
          ), 
          newBundles.flatten.toSet,
          tuplePresence
        )
      }

      case Select(args, child) => 
        val (newChild, childBundles, tuplePresence) = rewrite(child)



      case _ if oper.expression == Nil || CTables.isDeterministic(oper) => 
        oper.recur(rewrite(_))

    }
  }

}

class TupleBundleEval(schema:List[(String, Type.T)], childBundles: Set[String], expr: Expression) 
  extends SimpleMimirFunction()
{
  val inlined = 
    Eval.inline(expr, 
      schema.zipWithIndex.map({ case ((name,t),idx) =>
        (name, SQLiteValPlaceholder(this, idx, t))
      })
    )

  def xFunc(): Unit = 
  {
    return_mimir(Eval.eval(inlined))
  }
}

class SQLiteValPlaceholder(fn: MimirFunction, idx: Int, t: Type.T) extends Proc(Nil)
{
  def getType(argTypes: List[Type.T]): Type.T = t 
  def get(v: List[PrimitiveValue]) = fn.value_mimir(idx, t)
}

case class ValueBundle(t:Type.T) extends PrimitiveValue(t) {

  val v = new Array[PrimitiveValue](TupleBundles.SAMPLE_COUNT)

  def init: 

  def asLong: Long = throw new SQLException("Can't cast value bundle to Long")
  def asDouble: Double = throw new SQLException("Can't cast value bundle to Double")

  def asString: String = v.mkString(",")
  def payload: Object = v

}

