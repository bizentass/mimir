package mimir.deprecated

import mimir._
import mimir.algebra._
import mimir.optimizer._
import mimir.exec._
import mimir.ctables._

class OldCompiler(db: Database)
{

  def optimize(oper: Operator, opt:List[(Operator => Operator)]): Operator =
    opt.foldLeft(oper)((o, fn) => fn(o))

  def compileClassic(oper: Operator): ResultIterator =
  {
    val optOper = optimize(oper, List(
        OldPercolator.percolate _,
        InlineProjections.optimize _
      ))

    buildNonDetIterator(optOper)
  }

  def compilePartitioned(oper: Operator): ResultIterator =
  {
    val optOper = optimize(oper, List(
        OldPercolator.percolate _,
        CTPartition.partition _,
        InlineProjections.optimize _
      ))

    buildNonDetIterator(optOper)
  }

  def buildNonDetIterator(oper: Operator): ResultIterator = {
    if (CTables.isProbabilistic(oper)) {
      oper match {
        case Project(cols, src) =>
          val inputIterator = buildNonDetIterator(src);
          // println("Compiled ["+inputIterator.schema+"]: \n"+inputIterator)
          new ProjectionResultIterator(
            db,
            inputIterator,
            cols.map((x) => (x.name, x.expression))
          );

        case mimir.algebra.Union(lhs, rhs) =>
          new BagUnionResultIterator(
            buildNonDetIterator(lhs),
            buildNonDetIterator(rhs)
          );

        case _ => ???
      }
    } else {
      db.compiler.compile(oper)
    }
  }

}