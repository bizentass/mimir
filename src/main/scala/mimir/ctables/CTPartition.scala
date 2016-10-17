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

	def partition(oper: Operator): List[(Map[String,Set[String]], Set[String], Operator)] =
	{
		if(CTables.isDeterministic(oper)){
			return List( (oper.schema.map(_._1).map( (_, Set[String]()) ).toMap, Set(), oper) )
		}
		oper match {
			case Project(args, child) =>
				val inputPartitions = partition(child)

				val perArgPartitions =
					args.map( (arg) => {
						findPivots(arg.expression).toList.map({ case (partitionVars, pivots) => 
							(
								arg.name, 
								constructPartition(arg.expression, pivots), 
								partitionVars, 
								pivots
							)
						})
					})

				val perPartitionArgs =
					ListUtils.powerList(perArgPartitions)

				inputPartitions.flatMap({ case (childArgVGDeps, childRowVGDeps, childPartition) => 
					perPartitionArgs.map( (myArgs) => {
						val myArgDeps = 
							myArgs.map({ case (name, expr, myVars, _) => 
								(name, myVars.toSet ++ ExpressionUtils.getColumns(expr).flatMap(childArgVGDeps(_)))
							}).toMap
						val myArgDefns =
							myArgs.map({ case (name, expr, _, _) => 
								ProjectArg(name, expr)
							})
						val myArgConditions = myArgs.flatMap(_._4)

						( 
							myArgDeps, childRowVGDeps, 
							Project(myArgDefns, 
								OperatorUtils.applyFilter(myArgConditions, childPartition)
							)
						)
					})
				}) 

			case Select(cond, child) => {

				val inputPartitions = partition(child)
				val pivots = 
					findPivots(cond).toList


				inputPartitions.flatMap({ case (childArgVGDeps, childRowVGDeps, childPartition) => 
				  pivots.map({ case (partitionVars, pivots) => 

				  	val newCond = 
				  		constructPartition(cond, pivots)

				  	val newRowDeps = 
				  		childRowVGDeps ++ ExpressionUtils.getColumns(newCond).flatMap(childArgVGDeps(_))

				  	( childArgVGDeps, newRowDeps, Select(cond, childPartition) )

			  	})
				})
			}

			case Join(lhs, rhs) => {

				val lhsPartitions = partition(lhs)
				val rhsPartitions = partition(rhs)

				lhsPartitions.flatMap({ case (lhsArgVGDeps, lhsRowVGDeps, lhsPartition) =>
					rhsPartitions.map({ case (rhsArgVGDeps, rhsRowVGDeps, rhsPartition) =>
						(
							lhsArgVGDeps ++ rhsArgVGDeps, 
							lhsRowVGDeps ++ rhsRowVGDeps,
							Join(lhsPartition, rhsPartition)
						)
					})
				})
			}

			case Union(lhs, rhs) => {
				val lhsPartitions = partition(lhs)
				val rhsPartitions = partition(rhs)

				lhsPartitions++rhsPartitions				
			}

		}

	}


}