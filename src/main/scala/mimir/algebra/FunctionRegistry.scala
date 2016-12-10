package mimir.algebra;

import java.sql.SQLException

import mimir.provenance._
import mimir.ctables._
import mimir.util._

case class RegisteredFunction(
	fname: String, 
	evaluator: List[PrimitiveValue] => PrimitiveValue, 
	typechecker: List[Type] => Type
) {
	def getName = fname;
	def typecheck(args: List[Type]) = typechecker(args)
	def eval(args: List[PrimitiveValue]) = evaluator(args)
}

object FunctionRegistry {
	
	var functionPrototypes: scala.collection.mutable.Map[String, RegisteredFunction] = 
		scala.collection.mutable.Map.empty;

	{
		registerFunction("MIMIR_MAKE_ROWID", 
      Provenance.joinRowIds(_: List[PrimitiveValue]),
			((args: List[Type]) => 
				if(!args.forall( t => (t == TRowId()) || (t == TAny()) )) { 
					throw new TypeException(TAny(), TRowId(), "MIMIR_MAKE_ROWID")
				} else {
					TRowId()
				}
			)
		)

    registerFunction("__LIST_MIN", 
    	(params: List[PrimitiveValue]) => {
        FloatPrimitive(params.map( x => 
          try { x.asDouble } 
          catch { case e:TypeException => Double.MaxValue }
        ).min)
      },
    	{ (x: List[Type]) => 
    		Typechecker.assertNumeric(Typechecker.escalate(x), Function("__LIST_MIN", List())) 
    	}
    )

    registerFunction("__LIST_MAX", 
    	(params: List[PrimitiveValue]) => {
        FloatPrimitive(params.map( x => 
          try { x.asDouble } 
          catch { case e:TypeException => Double.MinValue }
        ).max)
      },
      { (x: List[Type]) => 
    		Typechecker.assertNumeric(Typechecker.escalate(x), Function("__LIST_MAX", List())) 
    	}
    )

    registerFunctionSet(List("CAST", "MIMIRCAST"), 
      (params: List[PrimitiveValue]) => {
        params match {
          case x :: TypePrimitive(t)    :: Nil => Cast(t, x)
          case _ => throw new SQLException("Invalid cast: "+params)
        }
      },
      (_) => TAny()
    )

		registerFunctionSet(List("DATE", "TO_DATE"), 
		  (params: List[PrimitiveValue]) => 
          { TextUtils.parseDate(params.head.asString) },
		  _ match {
		    case TString() :: Nil => TDate()
		    case _ => throw new SQLException("Invalid parameters to DATE()")
		  }
		)

		registerFunction("ABSOLUTE", 
			{
	      case List(IntPrimitive(i))   => if(i < 0){ IntPrimitive(-i) } else { IntPrimitive(i) }
	      case List(FloatPrimitive(f)) => if(f < 0){ FloatPrimitive(-f) } else { FloatPrimitive(f) }
	      case List(NullPrimitive())   => NullPrimitive()
	      case x => throw new SQLException("Non-numeric parameter to absolute: '"+x+"'")
	    },
			(x: List[Type]) => Typechecker.assertNumeric(x(0), Function("ABSOLUTE", List()))
		)

    registerFunction("SQRT",
      {
        case List(n:NumericPrimitive) => FloatPrimitive(Math.sqrt(n.asDouble))
      },
      (x: List[Type]) => Typechecker.assertNumeric(x(0), Function("ABSOLUTE", List()))
    )

    registerFunction("BITWISE_AND", (x) => IntPrimitive(x(0).asLong & x(1).asLong), (_) => TInt())

    registerFunction("JSON_EXTRACT",(_) => ???, (_) => TAny())
    registerFunction("JSON_ARRAY_LENGTH",(_) => ???, (_) => TInt())
	}

	def registerFunctionSet(
		fnames: List[String], 
		eval:List[PrimitiveValue] => PrimitiveValue, 
		typechecker: List[Type] => Type
	): Unit =
		fnames.map(registerFunction(_, eval, typechecker))

	def registerFunction(
		fname: String, 
		eval: List[PrimitiveValue] => PrimitiveValue, 
		typechecker: List[Type] => Type
	): Unit =
		functionPrototypes.put(fname, RegisteredFunction(fname, eval, typechecker))

	def typecheck(fname: String, args: List[Type]): Type = 
		functionPrototypes(fname).typecheck(args)

	def eval(fname: String, args: List[PrimitiveValue]): PrimitiveValue =
		functionPrototypes(fname).eval(args)

}