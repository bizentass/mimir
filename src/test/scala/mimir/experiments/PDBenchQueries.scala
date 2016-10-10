package mimir.experiments;

import java.io._
import scala.collection.JavaConversions._
import org.specs2.mutable._

import mimir._;
import mimir.algebra._;
import mimir.util._;

/**
 * Evaluation based on PDBench (pdbench.sourceforge.net)
 */

object PDBenchQueries
  extends SQLTestSpecification("PDBenchExperiments", Map( 
    "jdbc" -> "sqlite-inline"
  ))
{
  
  val tables = Map(
    "NATION" -> "../PDBench-Data/nation.tbl", 
    "REGION" -> "../PDBench-Data/region.tbl",
    "SUPPLIER" -> "../PDBench-Data/supp.tbl"
  )

  sequential

  "PDBench" should {
    "be loadable" >> {

      exec("test/tpch_queries/tpch_schema.sql")

      tables.keys.foreach( (table) => 
        db.backend.update(s"ALTER TABLE $table ADD COLUMN tid INT")
      )

      tables.toList.foreach({ case (table, file) => 
        LoadCSV.handleLoadTable(db, table, new File(file), "\\|")
      })

      db.backend.resultRows(
        "SELECT N_NAME FROM NATION;"
      ).flatten must contain(eachOf[PrimitiveValue](
        StringPrimitive("ALGERIA"), 
        StringPrimitive("PERU"), 
        StringPrimitive("JAPAN")
      ))
    }

    "support the creation of RepairKey lenses" >> {
      tables.keys.foreach( (source) => {
        val target = source+"_CLEAN"
        lens(s"""
          CREATE LENS $target
            AS SELECT * FROM $source
            WITH REPAIR_KEY('TID')
        """)
      })

      val result = query("""
          SELECT TID, N_NAME, R_NAME, N_COMMENT 
          FROM NATION_CLEAN, REGION_CLEAN 
          WHERE N_REGIONKEY = R_REGIONKEY;
        """).mapRows( (x) => ( x(0), x(1), x(2), x.deterministicCol(3) ) )
      result must have size(25)
      result.filter(_._4) must have size(23)

    }
  }

}