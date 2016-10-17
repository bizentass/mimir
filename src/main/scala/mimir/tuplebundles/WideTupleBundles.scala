package mimir.tuplebundle

import java.sql.SQLException
import java.io._
import java.util.Random

import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir._
import mimir.algebra._
import mimir.ctables._
import mimir.sql.sqlite._

class WideTupleBundleRewriter(conn: java.sql.Connection) extends LazyLogging {

  val vgRewrite = new SQLiteVGTerms(conn)
  val defaultWorldBundle = (0 to TupleBundles.SAMPLE_COUNT).map( math.pow(2, _:Int).toInt ).fold(0)(_+_)

  def colForWorld(col: String, world: Int): String = 
    ("__TB_"+world+"_"+col)

  def explode(col: String) =
    (0 to TupleBundles.SAMPLE_COUNT).map(colForWorld(col, _))

  def explodeSchema(col: (String, Type.T)) =
    explode(col._1).map( (_, col._2) )

  def needsBundle(expr: Expression, childBundles:Set[String]) =
    (ExpressionUtils.getColumns(expr).exists( childBundles contains _ ) || 
      CTables.isProbabilistic(expr))

  def rewrite(expr: Expression, scope: ExpressionChecker, sampleIdx: Int, childBundles: Set[String]): Expression =
  {
    expr match {
      case VGTerm((modelName, model), idx, args) => 
        val newArgs = args.map(rewrite(_, scope, sampleIdx, childBundles))
        vgRewrite.sampleExpression(modelName, model, idx, args, IntPrimitive(sampleIdx), scope)
      case Var(v) if childBundles contains v => 
        Var(colForWorld(v, sampleIdx))
      case _ => expr.recur(rewrite(_, scope, sampleIdx, childBundles))
    }
  }

  def rewrite(oper: Operator): (Operator, Set[String]) =
  {
    logger.trace(s"Rewrite: $oper")
    val ret: (Operator, Set[String]) =
      oper match {
        case Project(args, child) => {
          val schema = Typechecker.typecheckerFor(child)
          val (newChild, childBundles) = rewrite(child)

          val bundlesNeeded = args.filter( (arg) => needsBundle(arg.expression, childBundles) ).map(_.name).toSet

          val newArgs = 
            args.flatMap( (col) => {
              if(bundlesNeeded contains col.name){
                explode(col.name).zipWithIndex.map({ case (name, idx) =>
                  ProjectArg(name, rewrite(col.expression, schema, idx, childBundles))
                })
              } else {
                List(col)
              }
            })

          val worldBundle = ProjectArg(TupleBundles.tupleBundleColumn, Var(TupleBundles.tupleBundleColumn))

          (
            Project(newArgs ++ List(worldBundle), newChild),
            bundlesNeeded
          )
        }

        case Select(condition, child) => 
          val schema = Typechecker.typecheckerFor(child)
          val (newChild, childBundles) = rewrite(child)

          if(needsBundle(condition, childBundles)){
            val newCondition =
              Function("BITWISE_AND",
                List(
                  ExpressionUtils.makeSum(
                    (0 to TupleBundles.SAMPLE_COUNT).map( (i) => {
                      Conditional(
                        rewrite(condition, schema, i, childBundles),
                        IntPrimitive(math.pow(2, i).toInt),
                        IntPrimitive(0)
                      )
                    }).toList
                  ),
                  Var(TupleBundles.tupleBundleColumn)
                )
              )
            (
              Select(
                Comparison(Cmp.Gt, Var(TupleBundles.tupleBundleColumn), IntPrimitive(0)),
                Project(
                  newChild.schema.map(_._1).filter( !_.equals(TupleBundles.tupleBundleColumn) ).
                    map( (col) => ProjectArg(col, Var(col)) ) ++ 
                      List(ProjectArg(TupleBundles.tupleBundleColumn, newCondition)),
                  newChild
                )
              ),
              childBundles
            )
          } else {  
            (Select(condition, newChild), childBundles)
          }

        case Join(lhs, rhs) =>
          val (newLHS, lhsBundles) = rewrite(lhs)
          val (newRHS, rhsBundles) = rewrite(rhs)

          (
            OperatorUtils.joinMergingColumns(
              List( (TupleBundles.tupleBundleColumn, 
                     (a,b) => { Function("BITWISE_AND", List(a, b)) }
                    ) ),
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
              TupleBundles.tupleBundleColumn, IntPrimitive(defaultWorldBundle),
              Aggregate(args, gb, newChild)
            ),
            Set[String]()
          )

        case t: Table =>
          (
            OperatorUtils.projectInColumn(
              TupleBundles.tupleBundleColumn, IntPrimitive(defaultWorldBundle),
              t
            ),
            Set[String]()
          )
      }
    logger.trace(s"Rewritten: $ret")
    ret
  }
}

class SummarizeWideTupleBundle(t: Type.T) extends 
  SimpleMimirFunction( (0 to TupleBundles.SAMPLE_COUNT).map( (_) => t ).toList )
{
  def apply(args: List[PrimitiveValue]): PrimitiveValue =
  {
    StringPrimitive(SummarizeTupleBundle.apply(args.toArray))
  }
}