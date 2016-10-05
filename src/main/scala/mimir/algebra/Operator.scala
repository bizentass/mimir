package mimir.algebra;

import java.sql.SQLException

/**
 * Abstract parent class of all relational algebra operators
 */
abstract class Operator 
{ 
  /**
   * Convert the operator into a string.  Because operators are
   * nested recursively, and can span multiple lines, Every line 
   * of output should be prefixed with the specified string.
   */
  def toString(prefix: String): String; 
  /**
   * The starting point for stringification is to have no indentation
   */
  override def toString() = this.toString("");

  /**
   * Return all of the child nodes of this operator
   */
  def children: List[Operator];
  /**
   * Return a new instance of the same object, but with the 
   * children replaced with the provided list.  The list must
   * be of the same size returned by children.  This is mostly
   * to facilitate recur, below
   */
  def rebuild(c: List[Operator]): Operator;
  /**
   * Perform a recursive rewrite.  
   * The following pattern is pretty common throughout Mimir:
   * def replaceFooWithBar(e:Expression): Expression =
   *   e match {
   *     case Foo(a, b, c, ...) => Bar(a, b, c, ...)
   *     case _ => e.recur(replaceFooWithBar(_))
   *   }
   * Note how specific rewrites are applied to specific patterns
   * in the tree, and recur is used to ignore/descend through 
   * every other class of object
   */
  def recur(f: Operator => Operator) =
    rebuild(children.map(f))

  /**
   * Convenience method to invoke the Typechecker
   */
  def schema: List[(String, Type.T)] = 
    Typechecker.schemaOf(this)

  /**
   * Return all expression objects that appear in this node
   */
  def expressions: List[Expression]

  /** 
   * Replace all of the expressions in this operator.  Like 
   * rebuild, this method expects expressions to arrive in
   * the same order as they're returned by the expressions 
   * method
   */
  def rebuildExpressions(x: List[Expression]): Operator

  /**
   * Apply a method to recursively rewrite all of the Expressions
   * in this object.
   */
  def recurExpressions(op: Expression => Expression): Operator =
    rebuildExpressions(expressions.map( op(_) ))

  /**
   * Apply a method to recursively rewrite all of the Expressions
   * in this object, with types available
   */
  def recurExpressions(op: (Expression, ExpressionChecker) => Expression): Operator =
  {
    val checker = 
      children match {
        case Nil         => new ExpressionChecker()
        case List(child) => Typechecker.typecheckerFor(child)
        case _           => throw new SQLException("Don't know how to get types for multiple columns")
    }
    recurExpressions(op(_, checker))
  }
}

/**
 * A single column output by a projection
 */
case class ProjectArg(name: String, expression: Expression) 
{
  override def toString = (name.toString + " <= " + expression.toString)
}

/**
 * Generalized relational algebra projection
 */
case class Project(columns: List[ProjectArg], source: Operator) extends Operator 
{
  def toString(prefix: String) =
    prefix + "PROJECT[" + 
      columns.map( _.toString ).mkString(", ") +
    "](\n" + source.toString(prefix + "  ") + 
      "\n" + prefix + ")"

  def children() = List(source);
  def rebuild(x: List[Operator]) = Project(columns, x.head)
  def get(v: String): Option[Expression] = 
    columns.find( (_.name == v) ).map ( _.expression )
  def bindings: Map[String, Expression] =
    columns.map( (x) => (x.name, x.expression) ).toMap
  def expressions = columns.map(_.expression)
  def rebuildExpressions(x: List[Expression]) = Project(
    columns.zip(x).map({ case (ProjectArg(name,_),expr) => ProjectArg(name, expr)}),
    source
  )
}

/* AggregateArg is a wrapper for the args argument in Aggregate case class where:
      function is the Aggregate function,
      column is/are the SQL table column(s) parameters,
      alias is the alias used for the aggregate column output,
      getOperatorName returns the operator name,
      getColumnName returns the column name
*/

/* to fix list: first, we need to get the correct aliases; second, we need to make Aggregate have a list of AggregateArgs;
third, we need to test and then branch in SqlToRa.scala (flat or agg select)
 */
case class AggregateArg(function: String, columns: List[Expression], alias: String)
{
  override def toString = (function.toString + "(" + columns.map(_.toString).mkString(", ") + ")" + ", " + alias)
  def getFunctionName() = function
  def getColumnNames() = columns.map(x => x.toString).mkString(", ")
  def getAlias() = alias.toString
}

/* Aggregate Operator refashioned 5/23/16, 5/31/16 */
case class Aggregate(args: List[AggregateArg], groupby: List[Expression], source: Operator) extends Operator
{
  def toString(prefix: String) =
    prefix + "AGGREGATE[" + args.map(_.toString).mkString("; ") + "]\n\t(Group By [" + groupby.map( _.toString ).mkString(", ") +
      "])\n\t\t(" + source.toString(prefix + " ") + prefix + ")"

  def children() = List(source)
  def rebuild(x: List[Operator]) = new Aggregate(args, groupby, x(0))
  //def getAliases() = args.map(x => x.getAlias())
  def expressions = groupby ++ args.flatMap(_.columns)
  def rebuildExpressions(x: List[Expression]) = {
    val newGroupBy = 
      groupby.zipWithIndex.map( _._2 ).map( x(_) )
    val newArgs =
      args.foldLeft((List[AggregateArg](), groupby.length))({ 
        case ((arglist, baseIdx),AggregateArg(fn, cols, alias)) => 
          (
            arglist ++ List(
              AggregateArg(fn, cols.zipWithIndex.map(_._2+baseIdx).map( x(_) ), alias)
            ),
            baseIdx + cols.length
          )
      })._1
    Aggregate(newArgs, newGroupBy, source)
  }
}

/**
 * Relational algebra selection
 */
case class Select(condition: Expression, source: Operator) extends Operator
{
  def toString(prefix: String) =
    prefix + "SELECT[" + condition.toString + "](\n" + source.toString(prefix+"  ") + 
                  "\n" + prefix + ")"

  def children() = List(source)
  def rebuild(x: List[Operator]) = new Select(condition, x(0))
  def expressions = List(condition)
  def rebuildExpressions(x: List[Expression]) = Select(x(0), source)
}

/**
 * Relational algebra cartesian product (I know, technically not an actual join)
 */
case class Join(left: Operator, right: Operator) extends Operator
{
  def toString(prefix: String) =
    // prefix + "Join of\n" + left.toString(prefix+"  ") + "\n" + prefix + "and\n" + right.toString(prefix+"  ")
    prefix + "JOIN(\n" + left.toString(prefix+"  ") + ",\n" + right.toString(prefix+"  ") + 
                  "\n" + prefix + ")"
  def children() = List(left, right);
  def rebuild(x: List[Operator]) = Join(x(0), x(1))
  def expressions = List()
  def rebuildExpressions(x: List[Expression]) = this
}

/**
 * Relational algebra bag union
 */
case class Union(left: Operator, right: Operator) extends Operator
{
  def toString(prefix: String) =
    prefix + "UNION(\n" +
        left.toString(prefix+"  ") + ",\n" + 
        right.toString(prefix+"  ") + "\n" + 
    prefix + ")";

  def children() = List(left, right)
  def rebuild(x: List[Operator]) = Union(x(0), x(1))
  def expressions = List()
  def rebuildExpressions(x: List[Expression]) = this
}

/**
 * A base relation (Table).
 *
 * Note that schema information is required to make Typechecking self-contained 
 * and database-independent
 *
 * Metadata columns are special implicit attributes used by many database 
 * backends.  They are specified as follows:
 *   (output, input, type)
 * Where:
 *   output: The name that the implicit attribute will be referenced by in the 
 *           output relation
 *   input:  An expression to extract the implicit attribute
 *   type:   The type of the implicit attribute.
 * For example: 
 *   ("MIMIR_ROWID", Var("ROWID"), Type.TRowId)
 * will extract SQL's implicit ROWID attribute into the new column "MIMIR_ROWID" 
 * with the rowid type.
 */
case class Table(name: String, 
                 sch: List[(String,Type.T)],
                 metadata: List[(String,Expression,Type.T)])
  extends Operator
{
  def toString(prefix: String) =
    prefix + name + "(" + (
      sch.map( { case (v,t) => v+":"+Type.toString(t) } ).mkString(", ") + 
      ( if(metadata.size > 0)
             { " // "+metadata.map( { case (v,e,t) => v+":"+Type.toString(t)+" <- "+e } ).mkString(", ") } 
        else { "" }
      )
    )+")" 
  def children: List[Operator] = List()
  def rebuild(x: List[Operator]) = Table(name, sch, metadata)
  def metadata_schema = metadata.map( x => (x._1, x._3) )
  def expressions = List()
  def rebuildExpressions(x: List[Expression]) = this
}

/**
 * A left outer join
 */
case class LeftOuterJoin(left: Operator, 
                         right: Operator,
                         condition: Expression)
  extends Operator
{
  def toString(prefix: String): String =
    prefix+"LEFTOUTERJOIN("+condition+",\n"+
      left.toString(prefix+"   ")+",\n"+
      right.toString(prefix+"   ")+"\n"+
    prefix+")"

  def children: List[Operator] = 
    List(left, right)
  def rebuild(c: List[Operator]): Operator =
    LeftOuterJoin(c(0), c(1), condition)
  def expressions: List[Expression] = List(condition)
  def rebuildExpressions(x: List[Expression]) = LeftOuterJoin(left, right, x(0))
}
