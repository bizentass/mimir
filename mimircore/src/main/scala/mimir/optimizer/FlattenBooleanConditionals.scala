package mimir.optimizer;

import mimir.algebra._;

object FlattenBooleanConditionals extends TopDownExpressionOptimizerRule {

	def applyOne(e: Expression): Expression =
	{
		e match {
			case Conditional(condition, thenClause, elseClause) 
				if Typechecker.weakTypeOf(e) == Type.TBool => 
					ExpressionUtils.makeOr(
						ExpressionUtils.makeAnd(condition, thenClause),
						ExpressionUtils.makeAnd(Not(condition), elseClause)
					)
			case _ => e
		}
			
	}
}