package mimir.lenses;

import java.sql.SQLException
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir._
import mimir.algebra._;
import mimir.ctables._;
import mimir.provenance._;
import mimir.optimizer._;

class BestGuessCache(db: Database) extends LazyLogging {

  val dataColumn = "MIMIR_DATA"
  def keyColumn(idx: Int) = "MIMIR_KEY_"+idx

  def joinDataColumn(termId: Int) = "MIMIR_BEST_GUESS_"+termId
  def joinKeyColumn(idx: Int, termId: Int) = "MIMIR_ARG_"+termId+"_KEY_"+idx


  def cacheTableForLens(lensName: String, varIdx: Int): String =
    lensName + "_CACHE_" + varIdx
  def cacheTableForTerm(term: VGTerm): String =
    cacheTableForLens(term.model._1, term.idx)
  def cacheTableDefinition(model: String, varIdx: Int, termId: Int): Table = {
    val cacheTableName = cacheTableForLens(model, varIdx)
    val sch = db.getTableSchema(cacheTableName) match {
      case Some(sch) => sch
      case None => throw new SQLException(s"No cache table for '$cacheTableName'")
    }
    
    val keyCols = (0 to (sch.length - 2)).map( joinKeyColumn(_, termId) ).toList
    val dataCols = List(joinDataColumn(termId))

    Table(
      cacheTableName,
      (keyCols++dataCols).zip(sch.map(_._2)),
      List()
    )
  }

  private def swapVGTerms(expr: Expression, termMap: List[(VGTerm, Int)]): Expression =
    expr match {
      case v:VGTerm => 
        termMap.find( _._1.equals(v) ) match {
          case Some((_, idx)) => Var(joinDataColumn(idx))
          case None => 
            throw new Exception("Assert Failed: Expecting to have a replacement for all VG terms")
        }
      case _ => expr.recur(swapVGTerms(_, termMap))
  }


  private def buildOuterJoins(expressions: List[Expression], src: Operator): 
    (List[Expression], Operator) =
  {
    assert(CTables.isDeterministic(src))
    val vgTerms = expressions.
      flatMap( CTables.getVGTerms(_) ).
      toSet.toList.
      zipWithIndex
    val typechecker = new ExpressionChecker(Typechecker.schemaOf(src).toMap)
    val newSrc = 
      vgTerms.foldLeft(src)({
        case (oldSrc, (v @ VGTerm((model,_),idx,args), termId)) =>
          LeftOuterJoin(oldSrc,
            cacheTableDefinition(model, idx, termId),
            ExpressionUtils.makeAnd(
              args.zipWithIndex.map(
                arg => {
                  val keyBase = Var(joinKeyColumn(arg._2, termId))
                  val key = 
                    typechecker.typeOf(arg._1) match {
                      case Type.TRowId => 
                        // We materialize rowids as Strings in the backing store.  As
                        // a result, we need to convince the typechecker that we're
                        // being sane here.
                        Function("CAST", List[Expression](keyBase, TypePrimitive(Type.TRowId)))
                      case _  =>
                        keyBase
                    }
                  Comparison(Cmp.Eq, key, swapVGTerms(arg._1, vgTerms))
                }
              )
            )
          )
      })
    val newExprs = 
      expressions.map( swapVGTerms(_, vgTerms) )

    logger.debug(s"REBUILT: Project[$newExprs](\n$newSrc\n)")
    assert(CTables.isDeterministic(newSrc))
    newExprs.foreach( e => assert(CTables.isDeterministic(e)) )

    ( newExprs, newSrc )
  }

  def rewriteToUseCache(oper: Operator): Operator = {
    logger.trace(s"Rewriting: $oper")
    oper match {
      case x if x.expressions.flatMap(CTables.getVGTerms(_)).isEmpty =>
        x.recur(rewriteToUseCache(_))

      case Project(cols, src) => {
        val (newCols, newSrc) = 
          buildOuterJoins(
            cols.map(_.expression), 
            rewriteToUseCache(src)
          )
        Project(
          cols.map(_.name).
               zip(newCols).
               map( col => ProjectArg(col._1, col._2)), 
          newSrc
        )
      }

      case Select(cond, src) => {
        val (newCond, newSrc) = 
          buildOuterJoins(
            List(cond), 
            rewriteToUseCache(src)
          )
        Project(
          src.schema.map(_._1).map( x => ProjectArg(x, Var(x))),
          Select(newCond.head, newSrc)
        )
      }

    }
  }

  def buildCache(lens: Lens) =
    recurCacheBuildThroughLensView(lens.view, lens.name)

  private def recurCacheBuildThroughLensView(view: Operator, lensName: String): Unit =
  {
    logger.debug(s"Next step of caching for $lensName")
    // Start with all the expressions in the current RA node.
    view.expressions.
      // Pick out all of the VG terms, along with the conditions under which they 
      // contaminate the result
      flatMap(CTAnalyzer.compileCausality(_)).
      map( (x) => { logger.trace(s"Causality: $x"); x }).
      // Pick out those belonging to the current lens
      filter({ case (_, VGTerm((termName:String, _), _, _)) => termName.equals(lensName) }).
      map( (x) => { logger.trace(s"After Model Name: $x"); x }).
      // We only need caches for data-dependent terms
      filter( _._2.args.exists(ExpressionUtils.isDataDependent(_)) ).
      map( (x) => { logger.trace(s"Surviving: $x"); x }).
      map({ case (cond, term) => ((term.model._2, term.idx, term.args), cond) }).
      groupBy( _._1 ).
      map({ case (term, groups) => (term, ExpressionUtils.makeOr(groups.map(_._2))) }).
      foreach({ case ((model, idx, args), cond) =>
        if(view.children.length != 1){ 
          throw new SQLException("Assertion Failed: Expecting operator with expressions to have only one child")
        }
        buildCache(lensName, model, idx, args, Select(cond, view.children.head))
      })

    // Finally recur to the child nodes
    view.children.foreach( recurCacheBuildThroughLensView(_, lensName) )
  }
  def buildCache(term: VGTerm, input: Operator): Unit =
    buildCache(term.model._1, term.model._2, term.idx, term.args, input)
  def buildCache(lensName: String, model: Model, varIdx: Int, args: List[Expression], input: Operator): Unit = {
    val cacheTable = cacheTableForLens(lensName, varIdx)
    // We inline VG terms as a temporary hack to deal with the fact that 
    // the TypeInference lens completely messes with the typechecker.
    val typechecker = new ExpressionChecker(Typechecker.schemaOf(InlineVGTerms.optimize(input)).toMap)

    logger.debug(s"Building cache on $lensName-$varIdx["+args.mkString(",")+"]")
    logger.debug(s"Using: $input")

    if(db.getTableSchema(cacheTable) != None) {
      return; 
      dropCacheTable(cacheTable)
    }

    val argTypes = args.map( typechecker.typeOf(_) )
    createCacheTable(cacheTable, model.varType(varIdx, argTypes), argTypes)
    var rowCount = 0;

    db.query(input).foreachRow(row => {
      rowCount += 1;
      val compiledArgs = args.map(Provenance.plugInToken(_, row.provenanceToken()))
      val tuple = row.currentTuple()
      logger.trace(s"Processing: $tuple")
      val dataArgs = compiledArgs.map(Eval.eval(_, tuple))
      val guess = model.bestGuess(varIdx, dataArgs)
      val updateQuery = 
          "INSERT INTO "+cacheTable+"("+dataColumn+
            dataArgs.zipWithIndex.
              map( arg => (","+keyColumn(arg._2)) ).
              mkString("")+
          ") VALUES (?"+
            dataArgs.map(_ => ",?").mkString("")+
          ")"
      if(rowCount % 100000 == 0){ logger.debug(s" --- Row: $rowCount"); }
      // println("BUILD: "+updateQuery)
      db.backend.update(
        updateQuery, 
        guess :: dataArgs
      )
    })
  }

  private def emptyCacheTable(cacheTable: String) =
    db.backend.update( "DELETE FROM "+cacheTable )

  private def dropCacheTable(cacheTable: String) =
    db.backend.update( "DROP TABLE "+cacheTable )

  private def createCacheTable(cacheTable: String, dataType: Type.T, cacheTypes: List[Type.T]) = {
    val keyCols =
      cacheTypes.zipWithIndex.map( 
        typeIndex => (keyColumn(typeIndex._2), typeIndex._1)
      )

    logger.debug(s"CREATING $cacheTable["+cacheTypes.mkString(",")+s"] -> $dataType")

    val dataCols = List( (dataColumn, dataType) )
    val tableDirectives = 
      (keyCols ++ dataCols).map( 
        col => { col._1+" "+Type.toString(col._2) }
      ) ++ List(
        "PRIMARY KEY ("+keyCols.map(_._1).mkString(", ")+")"
      )
    val sql =
      "CREATE TABLE "+cacheTable+"("+
        tableDirectives.map("\n"+_).mkString(",")+
      ")"

    db.backend.update( sql )
  }
}