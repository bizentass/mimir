package mimir.lenses

import java.io._
import mimir.algebra._
import mimir.util._
import mimir.ctables.{VGTerm}
import mimir.optimizer.{ResolveViews,InlineVGTerms,InlineProjections}
import mimir.test._

object LensManagerSpec extends SQLTestSpecification("LensTests") {

  sequential

  "The Lens Manager" should {

    "Be able to create and query missing value lenses" >> {
      update("CREATE TABLE R(A int, B int, C int);")
      loadCSV("R", new File("test/r_test/r.csv"))
      query("SELECT B FROM R").mapRows(_(0)) should contain(NullPrimitive())
      update("CREATE LENS SANER AS SELECT * FROM R WITH MISSING_VALUE('B')")
      query("SELECT B FROM SANER").mapRows(_(0)) should not contain(NullPrimitive())
    }

    "Produce reasonable views" >> {
      db.loadTable("CPUSPEED", new File("test/data/CPUSpeed.csv"))
      val resolved1 = InlineProjections(ResolveViews(db, db.getTableOperator("CPUSPEED")))
      resolved1 must beAnInstanceOf[Project]
      val resolved2 = resolved1.asInstanceOf[Project]
      val coresModel = db.models.getModel("CPUSPEED:CORES")

      // Make sure the model name is right.
      // Changes to the way the type inference lens assigns names will need to
      // be reflected above.  That's the only thing that should cause this test
      // to fail.
      coresModel must not be empty

      resolved2.get("CORES") must be equalTo(Some(
        Function("CAST", List(Var("CORES"), VGTerm(coresModel, 0, List())))
      ))

      coresModel.reason(0, List()) must contain("was of type INT")

      val coresGuess1 = coresModel.bestGuess(0, List())
      coresGuess1 must be equalTo(TypePrimitive(TInt()))

      val coresGuess2 = InlineVGTerms(VGTerm(coresModel, 0, List()))
      coresGuess2 must be equalTo(TypePrimitive(TInt()))


    }

    "Be able to create and query type inference lenses" >> {

      val baseTypes = db.bestGuessSchema(db.getTableOperator("CPUSPEED_RAW")).toMap
      baseTypes.keys must contain(eachOf("CORES", "FAMILY", "TECH_MICRON"))
      baseTypes must contain("CORES" -> TString())
      baseTypes must contain("FAMILY" -> TString())
      baseTypes must contain("TECH_MICRON" -> TString())


      val lensTypes = db.bestGuessSchema(db.getTableOperator("CPUSPEED")).toMap
      lensTypes.keys must contain(eachOf("CORES", "FAMILY", "TECH_MICRON"))
      lensTypes must contain("CORES" -> TInt())
      lensTypes must contain("FAMILY" -> TString())
      lensTypes must contain("TECH_MICRON" -> TFloat())

    }

  }  

}