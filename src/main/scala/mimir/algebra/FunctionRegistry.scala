package mimir.algebra;

import java.sql.SQLException

import mimir.provenance._
import mimir.algebra.Type._
import mimir.ctables._

case class RegisteredFunction(
	fname: String, 
	evaluator: List[PrimitiveValue] => PrimitiveValue, 
	typechecker: List[Type.T] => Type.T
) {
	def getName = fname;
	def typecheck(args: List[Type.T]) = typechecker(args)
	def eval(args: List[PrimitiveValue]) = evaluator(args)
}

object FunctionRegistry {
	
	var functionPrototypes: scala.collection.mutable.Map[String, RegisteredFunction] = 
		scala.collection.mutable.Map.empty;

	{
		registerFunction("MIMIR_MAKE_ROWID", 
      Provenance.joinRowIds(_: List[PrimitiveValue]),
			((args: List[Type.T]) => 
				if(!args.forall( t => (t == TRowId) || (t == TAny) )) { 
					throw new TypeException(TAny, TRowId, "MIMIR_MAKE_ROWID")
				} else {
					TRowId
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
    	{ (x: List[Type.T]) => 
    		Typechecker.assertNumeric(Typechecker.escalate(x)) 
    	}
    )

    registerFunction("__LIST_MAX", 
    	(params: List[PrimitiveValue]) => {
        FloatPrimitive(params.map( x => 
          try { x.asDouble } 
          catch { case e:TypeException => Double.MinValue }
        ).max)
      },
      { (x: List[Type.T]) => 
    		Typechecker.assertNumeric(Typechecker.escalate(x)) 
    	}
    )

    registerFunctionSet(List("CAST", "MIMIRCAST"), 
      (params: List[PrimitiveValue]) => {
        try {
          params match {
            case x :: TypePrimitive(TInt)    :: Nil => IntPrimitive(x.asLong)
            case x :: TypePrimitive(TFloat)  :: Nil => FloatPrimitive(x.asDouble)
            case x :: TypePrimitive(TString) :: Nil => StringPrimitive(x.asString)
            case _ :: t :: Nil => throw new SQLException("Unknown cast type: '"+t+"'")
            case _ => throw new SQLException("Invalid cast: "+params)
          }
        } catch {
          case _:TypeException=> NullPrimitive();
          case _:NumberFormatException => NullPrimitive();
        }
      },
      (_) => TAny
    )

		registerFunctionSet(List("DATE", "TO_DATE"), 
		  (params: List[PrimitiveValue]) => {
		     val date = params.head.asInstanceOf[StringPrimitive].v.split("-").map(x => x.toInt)
		     new DatePrimitive(date(0), date(1), date(2))
		   },
		  _ match {
		    case TString :: Nil => TDate
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
				(x: List[Type.T]) => Typechecker.assertNumeric(x(0))
			)
	}

	def registerFunctionSet(
		fnames: List[String], 
		eval:List[PrimitiveValue] => PrimitiveValue, 
		typechecker: List[Type.T] => Type.T
	): Unit =
		fnames.map(registerFunction(_, eval, typechecker))

	def registerFunction(
		fname: String, 
		eval: List[PrimitiveValue] => PrimitiveValue, 
		typechecker: List[Type.T] => Type.T
	): Unit =
		functionPrototypes.put(fname, RegisteredFunction(fname, eval, typechecker))

	def typecheck(fname: String, args: List[Type.T]): Type.T = 
		functionPrototypes(fname).typecheck(args)

	def eval(fname: String, args: List[PrimitiveValue]): PrimitiveValue =
		functionPrototypes(fname).eval(args)

}