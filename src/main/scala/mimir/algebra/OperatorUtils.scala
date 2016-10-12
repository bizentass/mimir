package mimir.algebra;

import java.sql.SQLException
import java.io._

object OperatorUtils {
    
  /** 
   * Strip the expression required to compute a single column out
   * of the provided operator tree.  
   * Essentially a shortcut for an optimized form of 
   *
   * Project( [Var(col)], oper )
   * 
   * is equivalent to (and will often be slower than): 
   *
   * Project(ret(0)._1, ret(0)._2) UNION 
   *     Project(ret(1)._1, ret(1)._2) UNION
   *     ...
   *     Project(ret(N)._1, ret(N)._2) UNION
   */
  def columnExprForOperator(col: String, oper: Operator): 
    List[(Expression, Operator)] =
  {
    oper match {
      case p @ Project(_, src) => 
        List[(Expression,Operator)]((p.bindings.get(col).get, src))
      case Union(lhs, rhs) =>
        columnExprForOperator(col, lhs) ++ 
        	columnExprForOperator(col, rhs)
      case _ => 
        List[(Expression,Operator)]((Var(col), oper))
    }
  }

  /**
   * Normalize an operator tree by distributing operators
   * over union terms.
   */
  def extractUnions(o: Operator): List[Operator] =
  {
    // println("Extract: " + o)
    o match {
      case Union(lhs, rhs) => 
        extractUnions(lhs) ++ extractUnions(rhs)
      case Project(args, c) =>
        extractUnions(c).map ( Project(args, _) )
      case Select(expr, c) =>
        extractUnions(c).map ( Select(expr, _) )
      case Aggregate(args, groupBy, child) =>
        extractUnions(child).map(Aggregate(args, groupBy, _))
      case t : Table => List[Operator](t)
      case Join(lhs, rhs) =>
        extractUnions(lhs).flatMap (
          (lhsTerm: Operator) =>
            extractUnions(rhs).map( 
              Join(lhsTerm, _)
            )
        )
    }
  }

  def makeUnion(terms: List[Operator]): Operator = 
  {
    terms match {
      case List() => throw new SQLException("Union of Empty List")
      case List(head) => head
      case head :: rest => Union(head, makeUnion(rest))
    }
  }

  def projectAwayColumn(column: String, oper: Operator): Operator =
  {
    Project(
      oper.schema.
        map(_._1).
        filter( !_.equalsIgnoreCase(column) ).
        map( x => ProjectArg(x, Var(x)) ),
      oper
    )
  }

  def projectInColumn(column: String, value: Expression, oper: Operator): Operator =
  {
    Project(
      (oper.schema.
              map(_._1)
              map( (x:String) => ProjectArg(x, Var(x)) ))++
        List(ProjectArg(column, value)),
      oper
    )
  }

  def replaceColumn(column: String, replacement: Expression, oper: Operator) =
  {
    Project(
      oper.schema.
        map(_._1).
        map(x => if(x.equalsIgnoreCase(column)){ ProjectArg(x, replacement) } else { ProjectArg(x, Var(x)) } ),
      oper
    );
  }

  def renameColumn(column: String, replacement: String, oper: Operator) =
  {
    Project(
      oper.schema.
        map(_._1).
        map(x => if(x.equalsIgnoreCase(column)){ ProjectArg(replacement, Var(column)) } else { ProjectArg(x, Var(x)) } ),
      oper
    );
  }

  def projectColumns(cols: List[String], oper: Operator) =
  {
    Project(
      cols.map( (col) => ProjectArg(col, Var(col)) ),
      oper
    )
  }

  def joinMergingColumns(cols: List[(String, (Expression,Expression) => Expression)], lhs: Operator, rhs: Operator) =
  {
    val allCols = lhs.schema.map(_._1).toSet ++ rhs.schema.map(_._1).toSet
    val affectedCols = cols.map(_._1).toSet
    val wrappedLHS = 
      Project(
        lhs.schema.map(_._1).map( x => 
          ProjectArg(x, if(affectedCols.contains(x)) { Var("__MIMIR_JOIN_LHS_"+x) } 
                        else { Var(x) }) ), 
        lhs
      )
    val wrappedRHS = 
      Project(
        rhs.schema.map(_._1).map( x => 
          ProjectArg(x, if(affectedCols.contains(x)) { Var("__MIMIR_JOIN_RHS_"+x) } 
                        else { Var(x) }) ), 
        rhs
      )
    Project(
      ((allCols -- affectedCols).map( (x) => ProjectArg(x, Var(x)) )).toList ++
      cols.map({
        case (name, op) =>
          ProjectArg(name, op(Var("__MIMIR_JOIN_LHS_"+name), Var("__MIMIR_JOIN_RHS_"+name)))

        }),
      Join(wrappedLHS, wrappedRHS)
    )
  }


  def applyFilter(condition: List[Expression], oper: Operator): Operator =
    applyFilter(condition.fold(BoolPrimitive(true))(ExpressionUtils.makeAnd(_,_)), oper)

  def applyFilter(condition: Expression, oper: Operator): Operator =
    condition match {
      case BoolPrimitive(true) => oper
      case _ => Select(condition, oper)
    }

  def serializeOperator(oper: Operator): Array[Byte] = 
  {
    val baos = new ByteArrayOutputStream();
    val oos = new ObjectOutputStream(baos);
    oos.writeObject(oper);
    return baos.toByteArray();
  }

  def deserializeOperator(s: Array[Byte]): Operator =
  {
    val bais = new ByteArrayInputStream(s);
    val ois = new ObjectInputStream(bais);
    return ois.readObject().asInstanceOf[Operator]
  }
}