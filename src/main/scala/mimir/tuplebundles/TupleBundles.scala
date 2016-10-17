package mimir.tuplebundle

import java.sql.SQLException
import java.io._
import java.util.Random

import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir._
import mimir.algebra._
import mimir.ctables._
import mimir.sql.sqlite._

object TupleBundles {

  val SAMPLE_COUNT = 10
  val tupleBundleColumn = "__MIMIR_TUPLEBUNDLE"
  val checkTupleBundleFn = "__MIMIR_CHECK_BUNDLE"
  val initTupleBundleFn = "__MIMIR_BUNINIT"
  val summarizeTupleBundleFn = "__MIMIR_BUNSUM"
  val summarizeWideTupleBundleFn = "__MIMIR_WIDEBUNSUM_"

  val empty = TupleBundles.serialize((0 to SAMPLE_COUNT).map ( (_) => BoolPrimitive(true) ).toArray)

  def init(conn: java.sql.Connection) = 
  {
    FunctionRegistry.registerFunction(initTupleBundleFn,
      (_) => { throw new MatchError() },
      (_) => Type.TBool
    )
    org.sqlite.Function.create(conn, initTupleBundleFn, InitTupleBundle)

    FunctionRegistry.registerFunction(checkTupleBundleFn,
      (x) => BoolPrimitive(x.asInstanceOf[ValueBundle].v.exists( { case BoolPrimitive(x) => x } )),
      (_) => Type.TBool
    )
    org.sqlite.Function.create(conn, checkTupleBundleFn, CheckTupleBundle)

    FunctionRegistry.registerFunction(summarizeTupleBundleFn,
      (x) => StringPrimitive(SummarizeTupleBundle(x.asInstanceOf[ValueBundle].v)),
      (_) => Type.TString
    )
    org.sqlite.Function.create(conn, summarizeTupleBundleFn, SummarizeTupleBundle)

    List(Type.TBool, Type.TString, Type.TDate, Type.TInt, Type.TFloat).foreach( t => {
      val fn = new SummarizeWideTupleBundle(t)
      FunctionRegistry.registerFunction(summarizeWideTupleBundleFn+t,
        (x) => StringPrimitive(SummarizeTupleBundle(x.toArray)),
        (_) => Type.TString
      )
      org.sqlite.Function.create(conn, summarizeWideTupleBundleFn+t, fn)
    })
  }

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

  def rewrite(oper: Operator, conn: java.sql.Connection): Operator = 
  {
    val (rewritten, bundles) = new TupleBundleRewriter(conn).rewrite(oper)
    Project(
      oper.schema.map({ case (name, t) => 
        ProjectArg(name, 
          if(bundles contains name) {
            Function(summarizeTupleBundleFn, List(Var(name)))
          } else { Var(name) }
        )
      }),
      rewritten
    )
  }

  def rewriteWide(oper: Operator, conn: java.sql.Connection): Operator = 
  {
    val rewriter = new WideTupleBundleRewriter(conn)
    val (rewritten, bundles) = rewriter.rewrite(oper)
    Project(
      oper.schema.map({ case (name, t) => 
        ProjectArg(name, 
          if(bundles contains name){
            Var(rewriter.explode(name).head)
            // Function(summarizeWideTupleBundleFn+t, rewriter.explode(name).map(Var(_)).toList)
          } else {
            Var(name)
          }
        )
      }),
      rewritten
    )
  }

  def rewriteParallel(oper: Operator, conn: java.sql.Connection, idCols: List[String]): Operator =
  {
    var schema = oper.schema.toMap
    var rewriter = new SQLiteVGTerms(conn)
    Project(
      oper.schema.map(_._1).map(x=>ProjectArg(x, Var(x))),
      Aggregate(
        (schema.keys.toSet -- idCols.toSet).toList.map(col => AggregateArg("NTH", List(Var(col), IntPrimitive(0)), col)),
        idCols.map(Var(_)),
        OperatorUtils.makeUnion(
          (0 to SAMPLE_COUNT).map( (i) =>
            rewriter.rewriteSample(oper, IntPrimitive(i))
          ).toList
        )
      )
    )
  }
}

class TupleBundleRewriter(conn: java.sql.Connection) extends LazyLogging {

  var tupleBundleIdx = 0;
  var sampleIdx = 0;


  def nextFname(): String = 
  {
    tupleBundleIdx += 1;
    "__MIMIR_BUNDLE_"+tupleBundleIdx
  }

  def forceRewrite(expr: Expression, inSchema: Map[String,Type.T], childBundles: Set[String]): Expression =
  {
    val schema = inSchema + (TupleBundles.tupleBundleColumn -> Type.TBool)
    val exprSchema = ExpressionUtils.getColumns(expr).toList.map( (x) => (x, schema(x)) )
    val fname = nextFname()
    val eval = new TupleBundleEval(exprSchema, childBundles + TupleBundles.tupleBundleColumn, expr)
    FunctionRegistry.registerFunction(
      fname,
      eval(_),
      (args: List[Type.T]) => new ExpressionChecker(exprSchema.map(_._1).zip(args).toMap + (TupleBundles.tupleBundleColumn -> Type.TBool)).typeOf(expr)
    )
    org.sqlite.Function.create(conn, fname, eval)
    Function(fname, exprSchema.map(_._1).map(Var(_)))
  }

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
      return (forceRewrite(expr, schema, childBundles), true)
    } else {
      return (expr, false)
    }
  }


  def rewrite(oper: Operator): (Operator, Set[String]) =
  {
    logger.trace(s"Rewrite: $oper")
    val ret =
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
              forceRewrite(ExpressionUtils.makeAnd(Var(TupleBundles.tupleBundleColumn), condition), child.schema.toMap, childBundles),
              newChild
            )
          ), childBundles)

        case Join(lhs, rhs) =>
          val (newLHS, lhsBundles) = rewrite(lhs)
          val (newRHS, rhsBundles) = rewrite(rhs)

          (
            OperatorUtils.joinMergingColumns(
              List( (TupleBundles.tupleBundleColumn, 
                     (a,b) => {
                        val blended = ExpressionUtils.makeAnd(a, b)
                        forceRewrite(blended, 
                          ExpressionUtils.getColumns(blended).map( (_, Type.TBool) ).toMap, 
                          ExpressionUtils.getColumns(blended).toSet
                        )
                      }
                    )),
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

        case t: Table =>
          (
            OperatorUtils.projectInColumn(
              TupleBundles.tupleBundleColumn,
              Function(TupleBundles.initTupleBundleFn, List()),
              t
            ),
            Set[String]()
          )
      }
    logger.trace(s"Rewritten: $ret")
    ret
  }
}

class TupleBundleEval(schema:List[(String, Type.T)], childBundles: Set[String], inputExpr: Expression) 
  extends MimirFunction with LazyLogging
{
  val inlinedExprs = 
    (0 to TupleBundles.SAMPLE_COUNT).map(rewrite(inputExpr, _)).toArray.par
  val extractor: List[()=>PrimitiveValue] = schema.zipWithIndex.map( {
    case ((name, t), i) =>
      if(childBundles.contains(name)) { () => {
          val blob = value_blob(i)
          val blobSize = blob.size
          logger.debug(s"Extracting Bundle $name (id: $i, size: $blobSize)")
          ValueBundle(TupleBundles.deserialize(blob))
        }
      } else { () => { logger.debug(s"Extracting Raw $name (id: $i, type: $t)"); value_mimir(i, t) } }
  })

  def eval(args: List[PrimitiveValue]): Array[PrimitiveValue] = 
  {
    val scope = schema.map(_._1).zip(args).toMap
    logger.trace(s"SCOPE: $scope")

    inlinedExprs.map( Eval.eval(_, scope) ).toArray
  }

  def apply(args: List[PrimitiveValue]): PrimitiveValue =
    ValueBundle(eval(args))

  def xFunc(): Unit =
  {
    try {
      logger.debug(s"EVAL: $inputExpr $childBundles")
      result(
        TupleBundles.serialize(
          eval( extractor.map( _() ) )
        )
      )
    } catch {
      case e:Throwable => {
        println(e)
        e.printStackTrace()
        System.exit(-1)
      }
    }

  }

  def rewrite(e: Expression, sampleIdx: Int): Expression =
  {
    e match {
      case Var(vn) if childBundles.contains(vn) => SelectValue(vn, sampleIdx)
      case VGTerm((name, model), idx, args) => SelectSample(args.map(rewrite(_, sampleIdx)), name, model, idx, sampleIdx)
      case _ => e.recur(rewrite(_, sampleIdx))
    }
  }
}

case class SelectValue(varName: String, sampleIdx: Int) extends Proc(List(Var(varName)))
{
  def getType(argTypes: List[Type.T]): Type.T = return argTypes(0)
  def get(v: List[PrimitiveValue]): PrimitiveValue =
  {
    v.head match {
      case bundle: ValueBundle => bundle.get(sampleIdx)
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
  sampleIdx: Int
) extends Proc(args)
{
  def getType(argTypes: List[Type.T]): Type.T = model.varType(idx, argTypes)
  def get(v: List[PrimitiveValue]): PrimitiveValue = 
  {
    val worldId = (sampleIdx+":"+name+"_"+v.map(_.toString).mkString("_"))
    // println(s"  SAMPLING FOR $name.$idx FROM WORLD: $worldId")
    val rnd = new Random(worldId.hashCode)
    val ret = model.sample(idx, rnd, v)
    // println("     DONE SAMPLING")
    ret
  }
  def rebuild(v: List[Expression]) = SelectSample(v, name, model, idx, sampleIdx)
}

object CheckTupleBundle extends org.sqlite.Function
{
  def xFunc(): Unit = 
  {
    if(
      TupleBundles.deserialize(value_blob(0)).
        exists( { case BoolPrimitive(x) => x } )
    ){ result(1) } else { result(0) }
  }
}

object InitTupleBundle extends org.sqlite.Function
{
  def xFunc(): Unit = 
  {
    // println("INIT!")
    result(TupleBundles.empty)
  }
}

object SummarizeTupleBundle extends org.sqlite.Function
{
  def apply(fields: Array[PrimitiveValue]): String =
  {
    fields(0).getType match {
      case Type.TBool => 
        val matched = 
          fields.map({ case BoolPrimitive(x) => if(x){ 1 } else { 0 } }).fold(0)(_+_)
        val pct = matched.toDouble / TupleBundles.SAMPLE_COUNT

        s"$pct %"

      case Type.TInt | Type.TFloat => 
        if(fields.toSet.size > 1){
          val numerics = fields.map(_.asDouble)
          val tot:Double = numerics.fold(0.0)(_+_)
          val expectation = tot / fields.length        
          val varSq = numerics.map(expectation - _).map(x => x*x).fold(0.0)(_+_)
          val variance = math.sqrt( varSq ) / fields.length
          s"$expectation ± $variance"
        } else {
          fields(0).asString
        }

      case Type.TString => 
        fields.map(_.asString).toSet.map((x:String) => "'"+x+"'").mkString(" OR ")

      case Type.TDate => 
        val dates = fields.map(_.asString).toSet.toList.sorted
        if(dates.size <= 1){ dates.mkString("") }
        else {
          dates(0) + " -- " + dates(dates.length-1) 
        }
    }

  }

  def xFunc(): Unit = 
  {
    try {
      result(apply(TupleBundles.deserialize(value_blob(0))))
    } catch {
      case e:Throwable => {
        println(e)
        e.printStackTrace()
        System.exit(-1)
      }
    }
  }
}

case class ValueBundle(val v: Array[PrimitiveValue]) extends PrimitiveValue(Type.TAny) {
  def asLong: Long = throw new SQLException("Can't cast value bundle to Long")
  def asDouble: Double = throw new SQLException("Can't cast value bundle to Double")

  def asString: String = v.mkString(",")
  def payload: Object = v

  def get(i: Int) = v(i)
}
