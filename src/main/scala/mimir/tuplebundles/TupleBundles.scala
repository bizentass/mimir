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
  var sampleIdx = 0;

  val tupleBundleColumn = "__MIMIR_SQLITE_TUPLEBUNDLE"

  def nextFname(): String = 
  {
    tupleBundleIdx += 1;
    "__MIMIR_SQLITE_TUPLEBUNDLE_"+tupleBundleIdx
  }

  def rewrite(expr: Expression, schema:Map[String,Type.T], childBundles: Set[String]): (Expression, Boolean) =
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
      val eval = new TupleBundleEval(schema, childBundles, expr)
      FunctionRegistry.registerFunction(
        fname,
        eval(_),
        (args: List[Type.T]) => ExprChecker(schema.zip(args).toMap).typecheck(expr)
      )
      org.sqlite.Function.create(conn, fname, eval)
      (Function(fname, schema.map(Var(_))), true)
    } else {
      (expr, false)
    }

  }


  def rewrite(oper: Operator): (Operator, Set[String]) =
  {
    oper match {
      case Project(args, child) => {
        val (newChild, childBundles) = rewrite(child)
        val (newArgs, newBundles) = args.map( (arg) => {
            val (newExpr, needsBundle) = rewrite(arg.expression)
            (
              ProjectArg(arg.name, newExpr), 
              if(needsBundle){ Some(arg.name) } else { None }
            )
          }).unzip
        (
          Project(
            ProjectArg(tupleBundleColumn, Var(tupleBundleColumn)) :: newArgs,
            newChild
          ), 
          newBundles.flatten.toSet
        )
      }

      case Select(args, child) => 
        val (newChild, childBundles) = rewrite(child)




      case _ if oper.expression == Nil || CTables.isDeterministic(oper) => 
        oper.recur(rewrite(_))

    }
  }
}

class TupleBundleEval(schema:List[(String, Type.T)], childBundles: Set[String], inputExpr: Expression) 
  extends MimirFunction(schema.map(_._2))
{
  val argNames = schema.map(_._1)
  val inlinedExpr = rewrite(inputExpr)
  val extractor: ( ()=>PrimitiveValue ) = argNames.zipWithIndex.map( {
    case (name, i) =>
      if(childBundles.contains(name)) { () => ExpressionUtils.deserializePrimitiveValue(value_text(i)) }
      else { () => value_mimir(i) }
  })

  var sampleIdx = 0

  def apply(args: List[PrimitiveValue]): PrimitiveValue = 
  {
    Eval.eval(inlined, argsNames.zip(args).toMap)
  }

  def xFunc(): Unit =
  {
    apply( extractor.map( _() ) )
  }

  def rewrite(e: Expression): Expression =
  {
    e match {
      case Var(vn) if childBundles.contains(vn) => SelectValue(vn, this)
      case VGTerm((name, model), idx, args) => SelectSample(args.map(rewrite(_)), name, model, idx, this)
      case _ => e.recur(rewrite(_))
    }
  }
}

class SelectValue(varName: String, ctx: TupleBundleEval) extends Proc(List(Var(varName)))
{
  def getType(argTypes: List[Type.T]): Type.T = return argTypes(0)
  def get(v: List[PrimitiveValue]): PrimitiveValue =
  {
    v.head match {
      case bundle: ValueBundle => bundle.get(ctx.sampleIdx)
      case x => x
    }
  }
}

class SelectSample(
  args: List[Expression], 
  name: String, 
  model: Model, 
  idx: Int, 
  ctx: TupleBundleEval
) extends Proc(args)
{
  def getType(argTypes: List[Type.T]): Type.T = model.varType(idx, argTypes)
  def get(v: List[PrimitiveValue]): PrimitiveValue = 
  {
    Random rnd = new Random((ctx.sampleIdx+":"+name+"_"+idx+"_"+v.map(_.toString).mkString("_")).hashCode)
    model.sample(idx, rnd, v)
  }
}

class CheckTupleBundle() extends org.sqlite.Function
{
  def xFunc(): Unit = 
  {
    ExpressionUtils.deserializePrimitiveValue(value_text(0)) match {
      case ValueBundle(_, v) =>
        if(v.exists( { case BooleanPrimitive(x) => x } ){ result(1) } else { result(0) }
    }
  }
}

class InitTupleBundle() extends org.sqlite.Function
{
  val empty = (0 to TupleBundles.SAMPLE_COUNT).map ( BooleanPrimitive(true) ).toArray

  def xFunc(): Unit = 
  {
    result(ExpressionUtils.serializePrimitiveValue(ValueBundle(Type.TBool, empty)))
  }
}

case class ValueBundle(t:Type.T, val v: Array[PrimitiveValue]) extends PrimitiveValue(t) {
  def asLong: Long = throw new SQLException("Can't cast value bundle to Long")
  def asDouble: Double = throw new SQLException("Can't cast value bundle to Double")

  def asString: String = v.mkString(",")
  def payload: Object = v

  def get(i: Int) => v(i)
}
