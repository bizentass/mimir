package mimir.util

import java.io.{File, FileReader, BufferedReader}
import java.sql.SQLException
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.Database
import mimir.algebra._
import mimir.algebra.Type._

import scala.collection.mutable.ListBuffer

object LoadCSV extends LazyLogging {

  def handleLoadTable(db: Database, targetTable: String, sourceFile: File): Unit =
    handleLoadTable(db, targetTable, sourceFile, ",")
  def handleLoadTable(db: Database, targetTable: String, sourceFile: File, sep: String): Unit =
  {
    val input = new BufferedReader(new FileReader(sourceFile))
    val firstLine = input.readLine()

    db.getTableSchema(targetTable) match {

      case Some(sch) =>
        if(headerDetected(firstLine)) {
          val schMap = sch.toMap
          val tgtSch = firstLine.split(sep).map(_.toUpperCase).map((x) => (x, schMap(x))).toList
          populateTable(db, input, targetTable, tgtSch, sep) // Ignore header since table already exists
        }
        else {
          populateTable(
            db,
            new BufferedReader(new FileReader(sourceFile)), // Reset to top
            targetTable,
            sch,
            sep
          )
        }

      case None =>
        if(headerDetected(firstLine)) {
          db.backend.update("CREATE TABLE "+targetTable+"("+
            firstLine.split(sep).map(_+" varchar").mkString(", ")+")")

          handleLoadTable(db, targetTable, sourceFile)
        }
        else {
          throw new SQLException("No header supplied for creating new table")
        }
    }
  }


  /**
   * A placeholder method for an unimplemented feature
   *
   * During CSV load, a file may have a header, or not
   */
  private def headerDetected(line: String): Boolean = {
    if(line == null) return false

    // TODO Detection logic

    true // For now, assume every CSV file has a header
  }

  def parse(s: String, t:Type.T): PrimitiveValue =
  {
    t match {
      case TInt => IntPrimitive(s.toLong)
      case TFloat => FloatPrimitive(s.toDouble)
      case TString => StringPrimitive(s)
      case TDate => {
        val YMD = s.split("-").map(_.toInt)
        DatePrimitive(YMD(0), YMD(1), YMD(2))
      }
    }
  }

  private def populateTable(db: Database,
                            src: BufferedReader,
                            targetTable: String,
                            sch: List[(String, Type.T)],
                            sep: String): Unit = {
    while(true){
      val line = src.readLine()
      if(line == null) { return }

      db.backend.update(
        "INSERT INTO "+targetTable+"("+sch.map(_._1).mkString(",")+
          ") VALUES ("+sch.map((_) => "?").mkString(",")+")",
        line.trim.
          split(sep).
          padTo(sch.size, "").
          zip(sch).
          map({ case (v, (col, t)) => 
            logger.debug(s"Parse $col ($t): $v")
            parse(v, t) 
          }).
          toList
      )
    }
  }
}
