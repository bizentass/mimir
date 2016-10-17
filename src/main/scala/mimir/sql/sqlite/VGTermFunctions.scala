package mimir.sql.sqlite;

import mimir.algebra._;
import mimir.ctables._;
import scala.collection._;
import scala.util._;

object SQLiteVGTerms {
  def bestGuess(o:Operator, conn: java.sql.Connection): Operator =
  {
    new SQLiteVGTerms(conn).rewriteBestGuess(o)
  }
}

class SQLiteVGTerms(conn:java.sql.Connection) {
  val functionRegistryCache = mutable.Set[String]()

  def bestGuessExpression(modelName: String, model: Model, idx: Int, inputArgs: List[Expression], scope: ExpressionChecker) =
  {
    val fname = "__MIMIR_SQLITE_GUESS_"+modelName+"_"+idx

    if(!(functionRegistryCache contains fname)) {
      val argTypes = inputArgs.map(scope.typeOf(_))
      val fn = new VGTermGuess(model, idx, argTypes)
      FunctionRegistry.registerFunction(
        fname,
        fn(_),
        model.varType(idx, _:List[Type.T])
      )
      org.sqlite.Function.create(conn, fname, fn)
      functionRegistryCache.add(fname)
    }

    Function(fname, inputArgs)
  }

  def rewriteBestGuess(e: Expression, scope: ExpressionChecker): Expression =
  {
    e match {

      case VGTerm((modelName, model), idx, inputArgs) =>
        val args = inputArgs.map(rewriteBestGuess(_, scope))
        bestGuessExpression(modelName, model, idx, inputArgs, scope)

      case _ => e.recur(rewriteBestGuess(_, scope))
    }

  }

  def rewriteBestGuess(o: Operator): Operator = {
    o.recurExpressions(rewriteBestGuess(_, _)).
      recur(rewriteBestGuess(_))
  }

  def sampleExpression(modelName: String, model: Model, idx: Int, inputArgs: List[Expression], worldId: Expression, scope: ExpressionChecker) =
  {
    val fname = "__MIMIR_SQLITE_SAMPLE_"+modelName+"_"+idx

    if(!(functionRegistryCache contains fname)) {
      val argTypes = inputArgs.map(scope.typeOf(_))
      val fn = new VGTermSample(model, modelName, idx, argTypes)
      FunctionRegistry.registerFunction(
        fname,
        fn(_),
        model.varType(idx, _:List[Type.T])
      )
      org.sqlite.Function.create(conn, fname, fn)
      functionRegistryCache.add(fname)
    }

    Function(fname, worldId :: inputArgs)
  }

  def rewriteSample(e: Expression, scope: ExpressionChecker, worldId: Expression): Expression =
  {
    e match {

      case VGTerm((modelName, model), idx, inputArgs) =>
        val args = inputArgs.map(rewriteSample(_, scope, worldId))
        sampleExpression(modelName, model, idx, inputArgs, worldId, scope)

      case _ => e.recur(rewriteSample(_, scope, worldId))
    }
  }

  def rewriteSample(o: Operator, worldId: Expression): Operator = {
    o.recurExpressions(rewriteSample(_, _, worldId)).
      recur(rewriteBestGuess(_))
  }
}

class VGTermGuess(
  model: Model, 
  idx: Int, 
  argTypes: List[Type.T]
) extends SimpleMimirFunction(argTypes) {

  def apply(args: List[PrimitiveValue]): PrimitiveValue = 
    model.bestGuess(idx, args)

}
class VGTermSample(
  model: Model, 
  name: String,
  idx: Int, 
  argTypes: List[Type.T]
) extends SimpleMimirFunction(Type.TInt :: argTypes) {
  val rand = new Random();
  val seedBase = name.hashCode
  
  def apply(args: List[PrimitiveValue]): PrimitiveValue =
  {
    rand.setSeed(seedBase * args.toString().hashCode())
    model.sample(idx, rand, args.tail)
  }

}