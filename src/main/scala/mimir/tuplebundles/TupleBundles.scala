package mimir.tuplebundle

import java.sql.SQLException
import java.io._
import java.util.Random

import mimir._
import mimir.algebra._
import mimir.ctables._
import mimir.sql.sqlite._

object TupleBundles {

  val SAMPLE_COUNT = 10
  val tupleBundleColumn = "__MIMIR_SQLITE_TUPLEBUNDLE"
  val checkTupleBundleFn = "__MIMIR_SQLITE_CHECK_BUNDLE"
  val initTupleBundleFn = "__MIMIR_SQLITE_INIT_BUNDLE"

  val empty = TupleBundles.serialize((0 to SAMPLE_COUNT).map ( (_) => BoolPrimitive(true) ).toArray)


  def serialize(v: Array[PrimitiveValue]): Array[Byte] = 
  {
    val baos = new ByteArrayOutputStream();
    val oos = new ObjectOutputStream(baos);
    oos.writeObject(v);
    return baos.toByteArray();
  }

  def deserialize(s: Array[Byte]): Array[PrimitiveValue] =
  {
    val bais = new ByteArrayInputStream(s);
    val ois = new ObjectInputStream(bais);
    return ois.readObject().asInstanceOf[Array[PrimitiveValue]]
  }

  
}

class TupleBundleRewriter(db: Database, conn: java.sql.Connection) {

  var tupleBundleIdx = 0;
  var sampleIdx = 0;


  def nextFname(): String = 
  {
    tupleBundleIdx += 1;
    "__MIMIR_SQLITE_TUPLEBUNDLE_"+tupleBundleIdx
  }

  def forceRewrite(expr: Expression, childBundles: Set[String]): Expression =
  {
    val schema = ExpressionUtils.getColumns(expr).toList
    val fname = nextFname()
    val eval = new TupleBundleEval(schema, childBundles, expr)
    FunctionRegistry.registerFunction(
      fname,
      eval(_),
      (args: List[Type.T]) => new ExpressionChecker(schema.zip(args).toMap).typeOf(expr)
    )
    org.sqlite.Function.create(conn, fname, eval)
    Function(fname, schema.map(Var(_)))
  }

  def forceRewrite(expr: Expression): Expression =
    forceRewrite(expr, ExpressionUtils.getColumns(expr))

  def rewrite(expr: Expression, schema:Map[String,Type.T], childBundles: Set[String]): (Expression, Boolean) =
  {
    val argVars = ExpressionUtils.getColumns(expr)
    val allBundles = childBundles+TupleBundles.tupleBundleColumn
    // Check to see if the expression needs to be rewritten as a tuple
    // bundle.  This is the case if...
    //  - The expression references an input that is itself a tuple bundle
    //  - The expression references a VGTerm
    if(  !((argVars & allBundles).isEmpty) || 
         (CTables.isProbabilistic(expr)) ){
      return (forceRewrite(expr, childBundles), true)
    } else {
      return (expr, false)
    }
  }


  def rewrite(oper: Operator): (Operator, Set[String]) =
  {
    oper match {
      case Project(args, child) => {
        val (newChild, childBundles) = rewrite(child)
        val (newArgs, newBundles) = args.map( (arg) => {
            val (newExpr, needsBundle) = rewrite(arg.expression, newChild.schema.toMap, childBundles)
            (
              ProjectArg(arg.name, newExpr), 
              if(needsBundle){ Some(arg.name) } else { None }
            )
          }).unzip
        (
          Project(
            ProjectArg(TupleBundles.tupleBundleColumn, Var(TupleBundles.tupleBundleColumn)) :: newArgs,
            newChild
          ), 
          newBundles.flatten.toSet
        )
      }

      case Select(condition, child) => 
        val (newChild, childBundles) = rewrite(child)
        (Select(
          Function(TupleBundles.checkTupleBundleFn, List(Var(TupleBundles.tupleBundleColumn))),
          OperatorUtils.projectInColumn(
            TupleBundles.tupleBundleColumn,
            forceRewrite(ExpressionUtils.makeAnd(Var(TupleBundles.tupleBundleColumn), condition)),
            child
          )
        ), childBundles)

      case Join(lhs, rhs) =>
        val (newLHS, lhsBundles) = rewrite(lhs)
        val (newRHS, rhsBundles) = rewrite(rhs)

        (
          OperatorUtils.joinMergingColumns(
            List( (TupleBundles.tupleBundleColumn, 
                   (a,b) => forceRewrite(ExpressionUtils.makeAnd(a, b))) ),
            newLHS, newRHS
          ),
          lhsBundles ++ rhsBundles
        )

      case Union(lhs, rhs) =>
        val (newLHS, lhsBundles) = rewrite(lhs)
        val (newRHS, rhsBundles) = rewrite(rhs)
        if(!(lhsBundles ++ rhsBundles).isEmpty) { throw new SQLException("Invalid Aggregate!") }

        (Union(newLHS, newRHS), Set[String]())

      case Aggregate(args, gb, child) =>
        val (newChild, childBundles) = rewrite(child)
        if(!childBundles.isEmpty) { throw new SQLException("Invalid Aggregate!") }
        
        (
          OperatorUtils.projectInColumn(
            TupleBundles.tupleBundleColumn,
            Function(TupleBundles.initTupleBundleFn, List()),
            Aggregate(args, gb, newChild)
          ),
          Set[String]()
        )
    }
  }
}

class TupleBundleEval(schema:List[String], childBundles: Set[String], inputExpr: Expression) 
  extends MimirFunction
{
  val inlinedExpr = rewrite(inputExpr)
  val extractor: List[()=>PrimitiveValue] = schema.zipWithIndex.map( {
    case (name, i) =>
      if(childBundles.contains(name)) { () => ValueBundle(TupleBundles.deserialize(value_blob(i))) }
      else { () => value_mimir(i) }
  })

  var sampleIdx = 0

  def eval(args: List[PrimitiveValue]): Array[PrimitiveValue] = 
  {
    val scope = schema.zip(args).toMap

    (0 to TupleBundles.SAMPLE_COUNT).map( (i) => {
      sampleIdx = i;
      Eval.eval(inlinedExpr, scope)
    }).toArray
  }

  def apply(args: List[PrimitiveValue]): PrimitiveValue =
    ValueBundle(eval(args))

  def xFunc(): Unit =
  {
    result(
      TupleBundles.serialize(
        eval( extractor.map( _() ) )
      )
    )
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

case class SelectValue(varName: String, ctx: TupleBundleEval) extends Proc(List(Var(varName)))
{
  def getType(argTypes: List[Type.T]): Type.T = return argTypes(0)
  def get(v: List[PrimitiveValue]): PrimitiveValue =
  {
    v.head match {
      case bundle: ValueBundle => bundle.get(ctx.sampleIdx)
      case x => x
    }
  }
  def rebuild(v: List[Expression]) = this
}

case class SelectSample(
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
    val rnd = new Random((ctx.sampleIdx+":"+name+"_"+v.map(_.toString).mkString("_")).hashCode)
    model.sample(idx, rnd, v)
  }
  def rebuild(v: List[Expression]) = SelectSample(v, name, model, idx, ctx)
}

class CheckTupleBundle() extends org.sqlite.Function
{
  def xFunc(): Unit = 
  {
    if(
      TupleBundles.deserialize(value_blob(0)).
        exists( { case BoolPrimitive(x) => x } )
    ){ result(1) } else { result(0) }
  }
}

class InitTupleBundle() extends org.sqlite.Function
{
  def xFunc(): Unit = 
  {
    result(TupleBundles.empty)
  }
}

case class ValueBundle(val v: Array[PrimitiveValue]) extends PrimitiveValue(Type.TAny) {
  def asLong: Long = throw new SQLException("Can't cast value bundle to Long")
  def asDouble: Double = throw new SQLException("Can't cast value bundle to Double")

  def asString: String = v.mkString(",")
  def payload: Object = v

  def get(i: Int) = v(i)
}
