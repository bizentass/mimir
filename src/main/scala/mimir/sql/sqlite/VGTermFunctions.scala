package mimir.sql.sqlite;

import mimir.algebra._;
import mimir.ctables._;
import scala.collection._;

object SQLiteVGTerms {
  def bestGuess(o:Operator, conn: java.sql.Connection): Operator =
  {
    new SQLiteVGTermsBestGuess(conn).rewrite(o)
  }
}

class SQLiteVGTermsBestGuess(conn:java.sql.Connection) {
  val functionCache = mutable.Map[(String,Int), String]()

  def rewrite(e: Expression, scope: ExpressionChecker): Expression =
  {
    e match {

      case VGTerm((modelName, model), idx, inputArgs) =>
        val args = inputArgs.map(rewrite(_, scope))

        functionCache.get((modelName,idx)) match {
          case Some(fname) => Function(fname, args)
          case None =>
            val argTypes = args.map(scope.typeOf(_))
            val fn = new VGTermGuess(model, idx, argTypes)
            val fname = "__MIMIR_SQLITE_VGTERM_"+modelName+"_"+idx
            FunctionRegistry.registerFunction(
              fname,
              fn(_),
              model.varType(idx, _:List[Type.T])
            )
            org.sqlite.Function.create(conn, fname, fn)
            functionCache.put((modelName,idx), fname)
            Function(fname, args)
        }

      case _ => e.recur(rewrite(_, scope))
    }

  }

  def rewrite(o: Operator): Operator = {
    o.recurExpressions(rewrite(_, _)).
      recur(rewrite(_))
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