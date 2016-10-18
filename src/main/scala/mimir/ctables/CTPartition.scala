package mimir.ctables

import java.sql.SQLException

import mimir._
import mimir.util._
import mimir.algebra._
import mimir.optimizer._
import mimir.deprecated.OldPercolator


object CTPartition {

	def recurToFindPivots(expr: Expression, lead: Set[Expression]): List[(Set[String], Set[Expression])] =
	{
		expr match {
			case Conditional(c, t, e) => 
				recurToFindPivots(t, lead + c) ++ 
					recurToFindPivots(e, lead + ExpressionUtils.makeNot(c))
			case VGTerm((name,_),idx,args) =>
				val node = name+"_"+idx
				args.flatMap(recurToFindPivots(_, lead)).map({ case (nodes, condition) => (nodes + node, condition) })
			case _ if expr.children.isEmpty => 
				List((Set[String](), lead))
			case _ => 
				expr.children.flatMap(recurToFindPivots(_, lead))
		}
	}

	def findPivots(expr: Expression): Map[Set[String], Set[Expression]] =
	{
		ListUtils.reduce(recurToFindPivots(expr, Set())).mapValues( _.flatten.toSet )
	}

	def constructPartition(expr: Expression, pivots: Set[Expression]): Expression =
	{
		expr match {
			case Conditional(c, t, _) if pivots contains c => constructPartition(t, pivots)
			case Conditional(c, _, e) if pivots contains ExpressionUtils.makeNot(c) => constructPartition(e, pivots)
			case _ => expr.recur(constructPartition(_, pivots))
		}
	}

	def vgTermID(term:VGTerm): String =
		term.model._1+"_"+term.idx

	def exprTermIDs(expr: Expression): Set[String] =
		CTables.getVGTerms(expr).map(vgTermID(_)).toSet

	def traceExprDependencies(oper: Operator, expr: Expression): Set[String] =
	{
		ExpressionUtils.getColumns(expr).toSet.
			flatMap(traceColumnDependencies(oper, _:String)) ++
				exprTermIDs(expr)
	}

	def traceColumnDependencies(oper: Operator, col: String): Set[String] = 
	{
		if(CTables.isDeterministic(oper)){ return Set[String]() }
		oper match {
			case p@Project(_,child) => traceExprDependencies(child, p.get(col).get)
			case Select(_,child)    => traceColumnDependencies(child, col)
			case Union(lhs, rhs)    => traceColumnDependencies(lhs, col) ++
														       traceColumnDependencies(rhs, col)
			case Join(lhs, rhs)     => {
					if(lhs.schema.map(_._1).toSet contains col){
						traceColumnDependencies(lhs, col)
					} else {
						traceColumnDependencies(rhs, col)
					}
				}
		}
	}

	def traceRowDependencies(oper: Operator): Set[String] =
	{
		if(CTables.isDeterministic(oper)){ return Set[String]() }
		oper match {
			case Select(cond, child) => traceExprDependencies(child, cond) ++ 
																	traceRowDependencies(child)
			case Join(lhs, rhs)      => traceRowDependencies(lhs) ++
																	traceRowDependencies(rhs)
			case Union(lhs, rhs)     => traceRowDependencies(lhs) ++
																	traceRowDependencies(rhs)
 		  case Project(_,child)    => traceRowDependencies(child)
		}
	}

	def partitionOne(oper: Operator, args: List[ProjectArg], rowCondition:Expression): List[Operator] =
	{
		findPivots(rowCondition).values.
			map( (pivots) => {
				val partitionCondition = 
					constructPartition(rowCondition, pivots)
				Mimir.ifEnabled("HYBRID", () => {
					PushdownSelections.optimize(
						Project(args, 
							Select(
								ExpressionUtils.makeAnd(partitionCondition :: pivots.toList),
								oper
							)
						)
					)
				}, () => {
					Project(args ++ List(ProjectArg(CTables.conditionColumn, partitionCondition)),
						Select(ExpressionUtils.makeAnd(pivots.toList), oper)
					)
				})
			}).toList
	}

	def partition(oper: Operator): Operator =
	{
		if(CTables.isDeterministic(oper)) { return oper; }
		OperatorUtils.makeUnion(
			OperatorUtils.extractUnions(oper).
				map(OldPercolator.percolate(_)).
				flatMap({ 
					case Project(args, child) => 
						val (condition, rest) = 
							args.partition( _.name.equals(CTables.conditionColumn) )

						partitionOne(
							oper, 
							rest, 
							ExpressionUtils.makeAnd(condition.map(_.expression))
						)
					})
		)
	}



}