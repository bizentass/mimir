package mimir.ctables

import java.sql.SQLException

import mimir.algebra._
import mimir.util._
import mimir.optimizer.PropagateConditions;


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

	def exprTermIDs(expr: Exprssion): Set[String] =
		CTables.getVGTerms(expr).map(vgTermID(_)).toSet

	def traceExprDependencies(oper: Operator, expr: Expression): Set[String] =
	{
		ExpressionUtils.getColumns(expr).toSet.
			flatMap(traceColumnDependencies(oper, _)) ++
				exprTermIDs(expr)
	}

	def traceColumnDependencies(oper: Operator, col: String): Set[String] = 
	{
		if(CTables.isDeterministic(oper)){ return Set[String]() }
		oper match {
			case p:Project       => traceExprDependencies(p.child, p.get(col))
			case Select(_,child) => traceColumnDependencies(child)
			case Union(lhs, rhs) => traceColumnDependencies(lhs, col) ++
															traceColumnDependencies(rhs, col)
			case Join(lhs, rhs)  => {
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
			case Select(cond, child) => traceExprDependencies(cond, child) ++ 
																	traceRowDependencies(child)
			case Join(lhs, rhs)      => traceRowDependencies(lhs) ++
																	traceRowDependencies(rhs)
			case Union(lhs, rhs)     => traceRowDependencies(lhs) ++
																	traceRowDependencies(rhs)
 		  case Project(_,child)    => traceRowDependencies(child)
		}
	}

	def partition(oper: Operator): List[(Set[String], Operator)] =
	{
		val allRowDeps = traceRowDependencies(oper)

		val allRowDepCombinations =
			ListUtils.powerSet(oper.toList)

		 


	}



}