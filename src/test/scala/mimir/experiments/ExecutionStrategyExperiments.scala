package mimir.experiments;

import java.io._
import scala.collection.JavaConversions._
import org.specs2.mutable._

import mimir._;
import mimir.util._;


object ExecutionStrategyExperiments
  extends SQLTestSpecification("ExecutionStrategyExperiments", Map( 
    "jdbc" -> "sqlite-inline"
  ))
{
  
  val productDataFile = new File("test/data/Product.sql");
  val reviewDataFiles = List(
      new File("test/data/ratings1.csv"),
      new File("test/data/ratings2.csv")
    )

  sequential

  "SQLite-Inline" should {
    "Be Initializable" >> {
      stmts(productDataFile).map( update(_) )
      db.loadTable(reviewDataFiles(0))
      db.loadTable(reviewDataFiles(1))
      db.backend.resultRows("SELECT * FROM PRODUCT;") must have size(6)
    }

    "Create and Query Lenses" >> {
      lens("""
        CREATE LENS CLEAN_RATINGS2
          AS SELECT * FROM RATINGS2
          WITH MISSING_VALUE('EVALUATION')
      """)
      query("SELECT * FROM CLEAN_RATINGS2;").allRows must have size(3)

    }
  }

}