package mimir.lenses

import java.io._
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
  val keyCols: List[String] = args.map(_.asInstanceOf[PrimitiveValue].asString).map(_.toUpperCase)
  val dependentCols: List[String] = 
    (source.schema.map(_._1).toSet -- keyCols.toSet).toList

  var repairs: List[Map[List[PrimitiveValue], List[PrimitiveValue]]] = null

  def lensType: String = "REPAIR_KEY"
  def schema() = source.schema

  val cacheTable = "MIMIR_REPAIR_KEY_"+name+"_GUESSES"

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
      Table(cacheTable, source.schema ++ dependentCols.map("__MIMIR_FD_COUNT_"+_).map((_, Type.TInt)), List())
    )

  def build(db: Database): Unit = 
  {
    logger.debug(s"Building Repair Key Lens $name")
    logger.trace(s"  `----> \n$source")
    val tables = db.backend.getAllTables().toSet
    val schemaMap = source.schema.toMap

    if(!(tables contains cacheTable)){
      logger.debug(s"Rebuilding cache table for $name");
      logger.debug(s"Source: $source")


      val create = "CREATE TABLE "+cacheTable+"("+
        (schema.map( (col) => col._1+" "+Type.toString(col._2) ) ++
          dependentCols.map( "__MIMIR_FD_COUNT_"+_+" int")).mkString(",")+
        ")";
      logger.debug(create)
      db.backend.update(create)

      val insertQuery = 
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
      val cols = (schema.map(_._1) ++ dependentCols.map("__MIMIR_FD_COUNT_"+_))
      val insertSQL = "SELECT "+cols.mkString(", ")+" FROM ("+db.ra.convert(insertQuery)+") SUBQ;"
      val insert = "INSERT INTO "+cacheTable+"("+(schema.map(_._1) ++ dependentCols.map("__MIMIR_FD_COUNT_"+_)).mkString(",")+") "+insertSQL

      logger.debug(insert)
      db.backend.update(insert)
    }

    repairs = dependentCols.zipWithIndex.map({ case (col:String,colIdx:Int) => 
      val repairTable = "MIMIR_REPAIR_KEY_"+name+"_"+colIdx
      if(!(tables contains repairTable)){
        logger.debug(s"Computing repairs for ... $name.$col")
        db.backend.update("CREATE TABLE "+repairTable+"("+
          keyCols.zipWithIndex.map({ case (col, idx) => "key_"+idx+" "+schemaMap(col) }).mkString(", ")+
          ", data "+Type.toString(schemaMap(col))+")"
        )

        val compileQuery = 
          OperatorUtils.projectColumns(
            keyCols ++ List(col),
            Select( 
              ExpressionUtils.makeAnd(
                Comparison(Cmp.Gt, Var("MIMIR_REPAIR_KEY_COUNT"), IntPrimitive(1)) ::
                keyCols.zipWithIndex.map({ case (col, idx) => Comparison(Cmp.Eq, Var(col), Var("MIMIR_REPAIR_KEY_KEY_"+idx)) })
              ),
              Join(
                Project( 
                  ProjectArg("MIMIR_REPAIR_KEY_COUNT", Var("MIMIR_REPAIR_KEY_COUNT")) :: 
                    keyCols.zipWithIndex.map( x => ProjectArg("MIMIR_REPAIR_KEY_KEY_"+x._2, Var(x._1)) ),
                  Aggregate(
                    List(AggregateArg("COUNT_DISTINCT", List(Var(col)), "MIMIR_REPAIR_KEY_COUNT")),
                    keyCols.map(Var(_)),
                    source
                  )
                ),
                source
              )
            )
          )
        logger.debug(s"   Compile Query: $compileQuery")

        val compileSql = db.ra.convert(compileQuery);
        compileSql.asInstanceOf[net.sf.jsqlparser.statement.select.PlainSelect].setDistinct(new net.sf.jsqlparser.statement.select.Distinct())

        db.backend.update("INSERT INTO "+repairTable+" "+compileSql);
      }

      logger.debug(s"Loading repairs for ... $name.$col")
      Mimir.ifEnabled("REPAIR-KEY-NOLOAD", () => { 
        Map[List[PrimitiveValue], List[PrimitiveValue]]()
      }, () => {
        val results = db.backend.execute("SELECT "+keyCols.zipWithIndex.map(_._2).map("key_"+_).mkString(", ")+", data FROM "+repairTable)
        JDBCUtils.extractAllRows(results, (keyCols ++ List(col)).map( schemaMap(_) )).
          map( _.splitAt(keyCols.length) ).
          foldLeft(List[(List[PrimitiveValue], List[PrimitiveValue])]())({
            case (groups, (key, data)) => 
              if(groups.isEmpty || !groups.head._1.equals(key)){
                logger.trace(s"Creating repair for $key -> e.g., $data")
                (key, List(data.head)) :: groups
              } else {
                (groups.head._1, data.head :: groups.head._2) :: groups.tail
              }
          }).
          toMap[List[PrimitiveValue], List[PrimitiveValue]]
      })
    })


  }

  def guessRepair(idx: Int, args: List[PrimitiveValue]): PrimitiveValue =
  {
    repairs(idx).get(args) match {
      case None => NullPrimitive()
      case Some(rowRepair) => {
        logger.trace(s"Repair: $args-$idx")
        rowRepair(0)
      }
    }
  }

}

class FDRepairModel(lens: RepairKeyLens) extends Model with LazyLogging {

  def bestGuess(idx: Int, args: List[PrimitiveValue]): PrimitiveValue = 
  {
    lens.guessRepair(idx, args)
  }

  override def bestGuessExpression(idx: Int, args:List[Expression]): Option[Expression] =
  {
    // Some(Var(lens.dependentCols(idx)))
    None
  }

  def reason(idx: Int, args: List[Expression]): String = 
  { 
    "There were multiple possible values for "+lens.name+"."+lens.dependentCols(idx)+
    " on the row where ("+lens.keyCols.mkString(", ")+
    ") is ("+args.map(_.toString).mkString(", ")+")"
  }
  def sample(idx: Int, randomness: Random, args: List[PrimitiveValue]): PrimitiveValue = 
  {
    val myRepairs = lens.repairs(idx)
    myRepairs.get(args).map( (possibilities) => {
      val sampleId = math.abs(randomness.nextInt()) % possibilities.length;
      logger.trace(s" Variable $idx, using repair $sampleId")
      possibilities(sampleId)
    }).getOrElse(NullPrimitive())
  }
  def varType(idx: Int, argTypes: List[Type.T]): Type.T = 
  {
    lens.source.schema.find(_._1 == lens.dependentCols(idx)).get._2
  }
}
