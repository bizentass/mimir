package mimir.lenses

import java.io.File
import java.sql._
import java.util

import mimir.algebra._
import mimir.ctables._

import scala.collection.JavaConversions._
import scala.util._

class RepairKeyLens(name: String, args: List[Expression], source: Operator)
  extends Lens(name, args, source)
{
  val keyCols: Set[String] = List(args.map(_.toString)).toSet
  val dependentCols: List[String] = (source.schema.map(_._1).toSet - keyCols).toList

  val view = 
    Project(
      keyCols.map( (col) => ProjectArg(col, Var(col)) ).toList ++
      dependentCols.zipWithIndex.map( { case (col, idx) =>
        ProjectArg(col, 
          Conditional( 
            Comparison(Cmp.Gt, Var("__MIMIR_FD_COUNT_"+col), IntPrimitive(1)),
            VGTerm((model, name), idx, keyCols.map(Var(_))),
            Var(col)
          )
        )
      }),
      Aggregate(
        dependentCols.flatMap( (col) => {
          List(
            AggregateArg("NTH", List(IntPrimitive(0), Var(col)), col),
            AggregateArg("COUNT_DISTINCT", List(Var(col)), "__MIMIR_FD_COUNT_"+col)
          )
        }).toList,
        keyCols.map(Var(_)).toList,
        source
      )
    )
}

class FDRepairModel(lens: MissingValueLens, name: String) extends Model {

}
