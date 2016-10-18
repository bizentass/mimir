package mimir.exec


import java.sql._
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir.sql.IsNullChecker
import mimir._
import mimir.algebra.Union
import mimir.algebra._
import mimir.ctables._
import mimir.optimizer._
import mimir.provenance._
import net.sf.jsqlparser.statement.select._

class Compiler(db: Database) extends LazyLogging {

  def standardOptimizations: List[Operator => Operator] = List(
    ProjectRedundantColumns(_),
    InlineProjections.optimize _,
    PushdownSelections.optimize _
  )

  /**
   * Perform a full end-end compilation pass.  Return an iterator over
   * the result set.
   */
  def compile(oper: Operator): ResultIterator =
    compile(oper, standardOptimizations)

  /**
   * Perform a full end-end compilation pass.  Return an iterator over
   * the result set.  Use only the specified list of optimizations.
   */
  def compile(oper: Operator, opts: List[Operator => Operator]): ResultIterator = 
  {
    var currentQuery = oper;


    // We'll need the pristine pre-manipulation schema down the line
    // As a side effect, this also forces the typechecker to run, 
    // acting as a sanity check on the query before we do any serious
    // work.
    val outputSchema = currentQuery.schema;

    // The names that the provenance compilation step assigns will
    // be different depending on the structure of the query.  As a 
    // result it is **critical** that this be the first step in 
    // compilation.  
    val (provenanceAwareOper, provenanceCols) = Provenance.compile(currentQuery)
    currentQuery = provenanceAwareOper

    // Tag rows/columns with provenance metadata
    val (taggedOper, colDeterminism, rowDeterminism) =
    Mimir.ifEnabled("NO-TAINT-TRACKING", ()=> {
      (
        currentQuery, 
        outputSchema.map((col) => (col._1, BoolPrimitive(true))).toMap, 
        BoolPrimitive(true)
      )
    }, () => {
      CTPercolator.percolateLite(currentQuery)
    })
    currentQuery = taggedOper

    // The deterministic result set iterator should strip off the 
    // provenance columns.  Figure out which columns need to be
    // kept.  Note that the order here actually matters.
    val tagPlusOutputSchemaNames = 
      outputSchema.map(_._1).toList ++
        colDeterminism.toList.flatMap( x => ExpressionUtils.getColumns(x._2)) ++ 
        ExpressionUtils.getColumns(rowDeterminism)

    // Clean things up a little... make the query prettier, tighter, and 
    // faster
    currentQuery = optimize(currentQuery, opts)

    Mimir.ifEnabled("PARTITION", () => {
      currentQuery = CTPartition.partition(currentQuery)
    })

    logger.debug(s"OPTIMIZED: $currentQuery")

    // Remove any VG Terms for which static best-guesses are possible
    // In other words, best guesses that don't depend on which row we're
    // looking at (like the Type Inference or Schema Matching lenses)
    currentQuery = InlineVGTerms.optimize(currentQuery)

    // Replace VG-Terms with their "Best Guess values"
    currentQuery = bestGuessQuery(currentQuery, provenanceCols)

    // We'll need it a few times, so cache the final operator's schema.
    // This also forces the typechecker to run, so we get a final sanity
    // check on the output of the rewrite rules.
    val finalSchema = currentQuery.schema

    // The final stage is to apply any database-specific rewrites to adapt
    // the query to the quirks of each specific target database.  Each
    // backend defines a specializeQuery method that handles this
    currentQuery = db.backend.specializeQuery(currentQuery)

    logger.debug(s"FINAL: $currentQuery")

    // We'll need to line the attributes in the output up with
    // the order in which the user expects to see them.  Build
    // a lookup table with name + position in the query being execed.
    val finalSchemaOrderLookup = 
      finalSchema.map(_._1).zipWithIndex.toMap

    logger.debug("Wrapping iterator");

    Mimir.ifEnabled("CLASSIC-ITERATOR", () => {
      new BagUnionResultIterator(
        OperatorUtils.extractUnions(currentQuery).
          toIndexedSeq.par.
          map({ case Project(args, src)  =>

            val sql = db.ra.convert(currentQuery)
            val results = 
              db.backend.execute(sql)

            new mimir.deprecated.ProjectionResultIterator(
              db, 
              new ResultSetIterator(
                results, 
                finalSchema.toMap,
                tagPlusOutputSchemaNames.map(finalSchemaOrderLookup(_)), 
                provenanceCols.map(finalSchemaOrderLookup(_))
              ),
              args.map(x=>(x.name, x.expression))
            )
          }).toList
      )

    }, () => {
      // Generate the SQL
      val sql = db.ra.convert(currentQuery)

      logger.debug(s"SQL: $sql")

      // Deploy to the backend
      val results = 
        db.backend.execute(sql)

      // And wrap the results.
      new NonDetIterator(
        new ResultSetIterator(results, 
          finalSchema.toMap,
          tagPlusOutputSchemaNames.map(finalSchemaOrderLookup(_)), 
          provenanceCols.map(finalSchemaOrderLookup(_))
        ),
        outputSchema,
        outputSchema.map(_._1).map(colDeterminism(_)), 
        rowDeterminism
      )
    })

  }

  /**
   * Remove all VGTerms in the query and replace them with the 
   * equivalent best guess values
   */
  def bestGuessQuery(oper: Operator, idCols: List[String]): Operator =
  {
    // Remove any VG Terms for which static best-guesses are possible
    // In other words, best guesses that don't depend on which row we're
    // looking at (like the Type Inference or Schema Matching lenses)
    val mostlyDeterministicOper =
      InlineVGTerms.optimize(oper)

    // Deal with the remaining VG-Terms.  
    val fullyDeterministicOper =
      if(db.backend.supportsInlineBestGuess()) {
        // The best way to do this is a database-specific "BestGuess" UDF.  
        db.backend.compileForBestGuess(mostlyDeterministicOper, idCols) 
      } else {
        // This UDF doesn't always exist... if so, fall back to the Guess Cache
        db.bestGuessCache.rewriteToUseCache(mostlyDeterministicOper)
      }

    // And return
    return fullyDeterministicOper
  }

  /**
   * Optimize the query
   */
  def optimize(oper: Operator): Operator = 
    optimize(oper, standardOptimizations)

  /**
   * Optimize the query
   */
  def optimize(oper: Operator, opts: List[Operator => Operator]): Operator =
    opts.foldLeft(oper)((o, fn) => fn(o))

}