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
  
  sequential

  "PDBench" should {
    "be loadable" >> {
      stmts(new File("test/tpch_queries/tpch_schema.sql")).
        map( update(_) )
      db.backend.update("ALTER TABLE NATION ADD COLUMN tid INT")

      LoadCSV.handleLoadTable(
        db, 
        "NATION",
        new File("../PDBench-Data/nation.tbl"),
        "\\|"
      )
      db.backend.resultRows(
        "SELECT N_NAME FROM NATION;"
      ).flatten must contain(eachOf[PrimitiveValue](
        StringPrimitive("ALGERIA"), 
        StringPrimitive("PERU"), 
        StringPrimitive("JAPAN")
      ))
    }

    "support the creation of RepairKey lenses" >> {
      lens("""
        CREATE LENS NATION_CLEAN
          AS SELECT * FROM NATION
          WITH REPAIR_KEY('TID')
      """)

      val result = 
        query(
          "SELECT TID, N_NAME, N_COMMENT FROM NATION_CLEAN;"
        ).mapRows( (x) => ( x(0), x(1), x.deterministicCol(2) ) )
      result must have size(25)
      result.filter(_._3) must have size(23)

    }
  }

}