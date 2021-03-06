package mimir.ctables;

import java.util.Random;
import com.typesafe.scalalogging.slf4j.LazyLogging

import mimir._
import mimir.algebra._
import mimir.provenance._
import mimir.exec._
import mimir.util._
import mimir.optimizer._
import mimir.models._

case class InvalidProvenance(msg: String, token: RowIdPrimitive) 
	extends Exception("Invalid Provenance Token ["+msg+"]: "+token);

abstract class Explanation(
	val reasons: List[Reason], 
	val token: RowIdPrimitive
) {
	def fields: List[(String, PrimitiveValue)]

	override def toString(): String = {
		(fields ++ List( 
			("Reasons", reasons.map("\n    "+_.toString).mkString("")),
			("Token", JSONBuilder.string(token.v))
		)).map((x) => x._1+": "+x._2).mkString("\n")
	}

	def toJSON(): String = {
		JSONBuilder.dict(
			fields.map( { case (k, v) => (k, JSONBuilder.prim(v)) } ) ++ 
			List(
				("reasons", 
					JSONBuilder.list(reasons.map( _.toJSON ) )),
				("token", JSONBuilder.string(token.v))
		))
	}
}

case class RowExplanation (
	val probability: Double, 
	override val reasons: List[Reason], 
	override val token: RowIdPrimitive
) extends Explanation(reasons, token) {
	def fields = List(
		("probability", FloatPrimitive(probability))
	)
}

class CellExplanation(
	val examples: List[PrimitiveValue],
	override val reasons: List[Reason], 
	override val token: RowIdPrimitive,
	val column: String
) extends Explanation(reasons, token) {
	def fields = List[(String,PrimitiveValue)](
		("examples", StringPrimitive(examples.map( _.toString ).mkString(", "))),
		("column", StringPrimitive(column))
	)
}

case class GenericCellExplanation (
	override val examples: List[PrimitiveValue],
	override val reasons: List[Reason], 
	override val token: RowIdPrimitive,
	override val column: String
) extends CellExplanation(examples, reasons, token, column) {
}

case class NumericCellExplanation (
	val mean: PrimitiveValue,
	val sttdev: PrimitiveValue,
	override val examples: List[PrimitiveValue],
	override val reasons: List[Reason], 
	override val token: RowIdPrimitive,
	override val column: String
) extends CellExplanation(examples, reasons, token, column) {
	override def fields = 
		List(
			("mean", mean),
			("stddev", sttdev)
		) ++ super.fields
}


class CTExplainer(db: Database) extends LazyLogging {

	val NUM_SAMPLES = 1000
	val NUM_EXAMPLE_TRIALS = 20
	val NUM_FINAL_EXAMPLES = 3
	val rnd = new Random();
	
	def explainRow(oper: Operator, token: RowIdPrimitive): RowExplanation =
	{
		val (tuple, _, provenance) = getProvenance(oper, token)
		val probability = 
			if(CTables.isDeterministic(provenance)){ 1.0 }
			else { 
				sampleExpression[(Int,Int)](provenance, tuple, NUM_SAMPLES, (0,0), 
					(cnt: (Int,Int), present: PrimitiveValue) => 
						present match {
							case NullPrimitive() => (cnt._1, cnt._2)
							case BoolPrimitive(t) => 
								( cnt._1 + (if(t){ 1 } else { 0 }),
								  cnt._2 + 1
								 )
						}
				) match { 
					case (_, 0) => -1.0
					case (hits, total) => hits.toDouble / total.toDouble
				}
			}
		logger.debug(s"tuple: $tuple\ncondition: $provenance\nprobability: $probability")

		RowExplanation(
			probability,
			getFocusedReasons(provenance, tuple).values.toList,
			token
		)
	}

	def explainCell(oper: Operator, token: RowIdPrimitive, column: String): CellExplanation =
	{
		logger.debug(s"ExplainCell INPUT: $oper")
		val (tuple, allExpressions, _) = getProvenance(oper, token)
		logger.debug(s"ExplainCell Provenance: $allExpressions")
		val expr = allExpressions.get(column).get
		val colType = Typechecker.typeOf(InlineVGTerms(expr), tuple.mapValues( _.getType ))

		val examples = 
			sampleExpression[List[PrimitiveValue]](
				expr, tuple, NUM_EXAMPLE_TRIALS, 
				List[PrimitiveValue](), 
				(_++List(_)) 
			).toSet.take(NUM_FINAL_EXAMPLES)

		colType match {
			case (TInt() | TFloat()) =>
				val (avg, stddev) = getStats(expr, tuple, NUM_SAMPLES)

				NumericCellExplanation(
					avg, 
					stddev, 
					examples.toSet.toList,
					getFocusedReasons(expr, tuple).values.toList,
					token, 
					column
				)

			case _ => 
				GenericCellExplanation(
					examples.toSet.toList,
					getFocusedReasons(expr, tuple).values.toList,
					token,
					column
				)
		}
	}

	def sampleExpression[A](
		expr: Expression, bindings: Map[String,PrimitiveValue], count: Int, 
		init: A, accum: ((A, PrimitiveValue) => A)
	): A = 
	{
		val sampleExpr = CTAnalyzer.compileSample(expr, Var(CTables.SEED_EXP))
        (0 until count).
        	map( (i) => 
        		try {
	        		Eval.eval(
	        			sampleExpr, 
		        		bindings ++ Map(CTables.SEED_EXP -> IntPrimitive(rnd.nextInt()))
		        	)
		        } catch {
		        	// Cases like the type inference lens might lead to the expression
		        	// being impossible to evaluate.  Treat these as Nulls
		        	case _:TypeException => NullPrimitive()
		        }
        	).
	        foldLeft(init)(accum)

	}

	def getStats(expr: Expression, tuple: Map[String,PrimitiveValue], desiredCount: Integer): 
		(PrimitiveValue, PrimitiveValue) =
	{
		val (tot, totSq, realCount) =
			sampleExpression[(PrimitiveValue, PrimitiveValue, Int)](
				expr, tuple, desiredCount,
				(IntPrimitive(0), IntPrimitive(0), 0),
				{ case ((tot, totSq, ct), v) => 
					try {
						(
							Eval.applyArith(Arith.Add, tot, v),
							Eval.applyArith(Arith.Add, totSq,
								Eval.applyArith(Arith.Mult, v, v)),
							ct + 1
						)
	        } catch {
	        	// Cases like the type inference lens might lead to the expression
	        	// being non-numeric.  Simply skip these values.
	        	case _:TypeException => (tot, totSq, ct)
	        }
				}
			)
		if(realCount > 0){
			val avg = Eval.applyArith(Arith.Div, tot, FloatPrimitive(realCount.toDouble))
			val stddev =
				Eval.eval(
					Function("SQRT", List(					
						Function("ABSOLUTE", List(
							Arithmetic(Arith.Sub, 
								Arithmetic(Arith.Div, totSq, FloatPrimitive(realCount.toDouble)),
								Arithmetic(Arith.Mult, avg, avg)
							)
						))
					)
				)
			)
			(avg, stddev)
		} else {
			(StringPrimitive("n/a"), StringPrimitive("n/a"))
		}
	}

	def getFocusedReasons(expr: Expression, tuple: Map[String,PrimitiveValue]):
		Map[String,Reason] =
	{
		logger.trace(s"GETTING REASONS: $expr")
		expr match {
			case v: VGTerm => Map(v.model.name -> v.reason(v.args.map(Eval.eval(_,tuple))))

			case Conditional(c, t, e) =>
				(
					if(Eval.evalBool(c, tuple)){
						getFocusedReasons(t, tuple)
					} else {
						getFocusedReasons(e, tuple)
					}
				) ++ getFocusedReasons(c, tuple);

			case Arithmetic(op @ (Arith.And | Arith.Or), a, b) =>
				(
					(op, Eval.evalBool(InlineVGTerms.inline(a), tuple)) match {
						case (Arith.And, true) => getFocusedReasons(b, tuple)
						case (Arith.Or, false) => getFocusedReasons(b, tuple)
						case _ => Map()
					}
				) ++ getFocusedReasons(a, tuple)

			case _ => 
				expr.children.
					map( getFocusedReasons(_, tuple) ).
					foldLeft(Map[String,Reason]())(_++_)
		}
	}

	def getProvenance(rawOper: Operator, token: RowIdPrimitive): 
		(Map[String,PrimitiveValue], Map[String, Expression], Expression) =
	{
		val oper = ResolveViews(db, rawOper)
		logger.debug(s"RESOLVED: $oper")

		// Annotate the query to produce a provenance trace
		val (provQuery, rowIdCols) = Provenance.compile(oper)

		// Generate a split token
		val rowIdTokenMap = Provenance.rowIdMap(token, rowIdCols)

		// Flatten the query out into a set of expressions and a row condition
		val (tracedQuery, columnExprs, rowCondition) = Tracer.trace(provQuery, rowIdTokenMap)

		logger.debug(s"TRACE: $tracedQuery")
		logger.debug(s"EXPRS: $columnExprs")
		logger.debug(s"ROW: $rowCondition")

		val inlinedQuery = db.compiler.bestGuessQuery(tracedQuery)

		logger.debug(s"INLINE: $inlinedQuery")

		val optQuery = db.compiler.optimize(inlinedQuery)

		val finalSchema = optQuery.schema

		val sqlQuery = db.ra.convert(optQuery)

		logger.debug(s"SQL: $sqlQuery")

		val results = db.backend.execute(sqlQuery)

		val baseData = JDBCUtils.extractAllRows(results, finalSchema.map(_._2))

		if(!baseData.hasNext){
			val resultRowString = baseData.map( _.mkString(", ") ).mkString("\n")
			logger.debug(s"Results: $resultRowString")
			throw new InvalidProvenance(""+baseData.size+" rows for token", token)
		}	

		val tuple = finalSchema.map(_._1).zip(baseData.next).toMap

		(tuple, columnExprs, rowCondition)
	}

	def delveToProjection(oper: Operator, token: RowIdPrimitive):
		(Map[String, Expression], Operator, RowIdPrimitive) =
	{
		oper match {
			case Union(lhs, rhs) =>
				val (newToken, isLHS) = stripUnionProvenance(token);
				delveToProjection(if(isLHS){ lhs } else { rhs }, newToken);
			case Project(targets, source) => 
				return (
					targets.map ( { case ProjectArg(n,e) => (n,e) } ).toMap,
					source, 
					token
				)
			case _ =>
				throw InvalidProvenance("PERCOLATE", token)
		}
	}

	def stripUnionProvenance(token: RowIdPrimitive): 
		(RowIdPrimitive, Boolean) =
	{
		val payload = token.payload.toString;
		if(payload.endsWith(".left")){
			return (RowIdPrimitive(payload.substring(0, payload.length() - 5)), true)
		}
		else if(payload.endsWith(".right")){
			return (RowIdPrimitive(payload.substring(0, payload.length() - 5)), false)
		}
		else {
			throw InvalidProvenance("UNION", token)
		}
	}
}