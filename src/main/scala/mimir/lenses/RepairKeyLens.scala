package mimir.lenses

import java.io.File
import java.sql._
import java.util

import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir._
import mimir.algebra._
import mimir.ctables._
import mimir.util._

import scala.collection.JavaConversions._
import scala.util._

class RepairKeyLens(name: String, args: List[Expression], source: Operator)
  extends Lens(name, args, source) with LazyLogging
{
  val keyCols: List[String] = args.map(_.asInstanceOf[PrimitiveValue].asString)
  val dependentCols: List[String] = 
    (source.schema.map(_._1).toSet -- keyCols.toSet).toList

  var repairs: Map[List[PrimitiveValue], List[List[PrimitiveValue]]] = null

  def lensType: String = "REPAIR_KEY"
  def schema() = source.schema

  def model =
    new FDRepairModel(this)

  def view: Operator = 
    Project(
      keyCols.map( (col) => ProjectArg(col, Var(col)) ).toList ++
      dependentCols.zipWithIndex.map( { case (col, idx) =>
        ProjectArg(col, 
          Conditional( 
            Comparison(Cmp.Gt, Var("__MIMIR_FD_COUNT_"+col), IntPrimitive(1)),
            VGTerm((name, model), idx, keyCols.map(Var(_))),
            Var(col)
          )
        )
      }),
      Aggregate(
        dependentCols.flatMap( (col) => {
          List(
            AggregateArg("NTH", List(Var(col), IntPrimitive(0)), col),
            AggregateArg("COUNT_DISTINCT", List(Var(col)), "__MIMIR_FD_COUNT_"+col)
          )
        }).toList,
        keyCols.map(Var(_)).toList,
        source
      )
    )

  def build(db: Database): Unit = 
  {
    logger.debug(s"Building Repair Key Lens $name")
    val whichKeysAreDupped =
      db.ra.convert(
        OperatorUtils.projectColumns(
          keyCols,
          Select(
            ExpressionUtils.makeOr(dependentCols.map( (col) => {
              Comparison(Cmp.Gt, Var(col), IntPrimitive(1))
            })),
            Aggregate(
              dependentCols.map( (col) => {
                AggregateArg("COUNT_DISTINCT", List(Var(col)), col)
              }),
              keyCols.map(Var(_)).toList,
              source
            )
          )
        )
      ).toString
    val dups = db.backend.resultRows(whichKeysAreDupped)
    val schMap = source.schema.toMap
    
    val repairsForKey = 
      db.ra.convert(
        OperatorUtils.projectColumns(
          dependentCols,
          Select(
            ExpressionUtils.makeAnd(keyCols.map( (col) => 
              Comparison(Cmp.Eq, Var(col), JDBCVar(schMap(col)))
            )),
            source
          )
        )
      ).toString

    repairs = dups.map( (keyRow) => {
      val candidates = 
        db.backend.resultRows(repairsForKey, keyRow)
      logger.debug(s"Finding repairs for $keyRow: $candidates")
      val rowRepair = 
        ListUtils.unzip( candidates ).
          map( _.flatten.toSet.toList )
      logger.debug(s"Registering repair for $keyRow: $rowRepair")      
      ( keyRow, rowRepair )
    }).toMap

  }

  def guessRepair(idx: Int, args: List[PrimitiveValue]): PrimitiveValue =
  {
    repairs.get(args) match {
      case None => NullPrimitive()
      case Some(rowRepair) => {
        logger.debug(s"Repair: $args-$idx -> $rowRepair")
        rowRepair(idx)(0)
      }
    }
  }

}

class FDRepairModel(lens: RepairKeyLens) extends Model with LazyLogging {

  def bestGuess(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = 
  {
    lens.guessRepair(idx, args)
  }
  def reason(idx: Int, args: List[Expression]): String = 
  { 
    "There were multiple possible values for "+lens.name+"."+lens.dependentCols(idx)+
    " on the row where ("+lens.keyCols.mkString(", ")+
    ") is ("+args.map(_.toString).mkString(", ")+")"
  }
  def sample(idx: Int, randomness: Random, args: List[PrimitiveValue]): PrimitiveValue = 
  {
    val possibilities = lens.repairs(args)(idx);
    possibilities(randomness.nextInt() % possibilities.length)
  }
  def varType(idx: Int, argTypes: List[Type.T]): Type.T = 
  {
    lens.source.schema.find(_._1 == lens.dependentCols(idx)).get._2
  }
}
