package mimir.sql.sqlite;

object SQLiteVGTerms {
  val rewrite(o:Operator, conn: java.sql.Connection): Operator =
  {
    new VGTermFunctionRewriter(conn).rewrite(o)
  }
}

class VGTermFunctionRewriter(conn:java.sql.Connection) {
  val functionCache = mutable.Map[(String,Int), String]()

  def rewrite(e: Expression, scope: ExpressionChecker): Expression =
  {
    e match {

      case VGTerm((modelName, model), idx, inputArgs) =>
        val args = inputArgs.map(rewrite(_, scope))

        functionCache.get((model,idx)) match {
          case Some(fname) => Function(fname, args)
          case None =>
            val fn = new VGTermFunction(model, idx, args.map(scope.typeOf(_)))
            val fname = "__MIMIR_VGTERM_"+modelName+"_"+idx
            org.sqlite.Function.create(conn, fname, fn)
            functionCache.put((modelName,idx), fname)
            Function(fname, args)
        }

      case _ => e.recur(rewrite(_, scope))
    }

  }

  def rewrite(o: Operator): Operator =
    val typer = Typechecker.typecheckerFor(o)
    o.recurExpressions(rewrite(_, typer)).
      recur(rewrite(_))
  }

}


class VGTermFunction(
  model: Model, 
  idx: Int, 
  argTypes: List[Type.T]
) extends org.sqlite.Function {

  @Override
  def xFunc(): Unit = {
    try {
      result(8000);
    } catch {
      case _: java.sql.SQLDataException => throw new java.sql.SQLDataException();
    }
  }


}