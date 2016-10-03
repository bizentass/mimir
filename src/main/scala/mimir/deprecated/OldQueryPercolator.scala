package mimir.ctables

import java.sql.SQLException

import mimir.algebra._
import mimir.util._
import mimir.optimizer._

object OldQueryPercolator {

  val ROWID_KEY = "ROWID_MIMIR"

  /**
   * Transform an operator tree into a union of
   * union-free, percolated subtrees.
   * Postconditions:   
   *   The root of the tree is a hierarchy of Unions
   *   The leaves of the Union hierarchy are...
   *     ... if the leaf is nondeterministic,
   *       then a Project node that may be uncertain
   *       but who's subtree is deterministic
   *     ... if the leaf has no uncertainty, 
   *       then an arbitrary deterministic subtree
   */
  def percolate(oper: Operator): Operator = {
    // println("Percolate: "+o)
    InlineProjections.optimize(
      OperatorUtils.extractUnions(
        propagateRowIDs(oper)
      ).map( percolateOne(_) ).reduceLeft( Union(_,_) )
    )
  }
  
  val CONDITION_COLUMN = "MIMIR_ROW_CONDITION"

  /*
   * Normalize a union-free operator tree by percolating 
   * uncertainty-creating projections up through the
   * operator tree.  Selection predicates based on 
   * probabilistic predicates are converted into 
   * constraint columns.  If necessary uncertainty-
   * creating projections are converted into non-
   * uncertainty-creating projections to allow 
   * the uncertain attributes to percolate up.
   */
  def percolateOne(o: Operator): Operator =
  {
    // println("percolateOne: "+o)
    val extractProject:
        Operator => ( List[(String,Expression)], Operator ) =
      (e: Operator) =>
        e match {
          case Project(cols, rest) => (
              cols.map( (x) => (x.column, x.input) ),
              rest
            )
          case _ => (
              e.schema.map(_._1).map( (x) => (x, Var(x)) ).toList,
              e
            )
        }
    val addConstraintCol = (e: Operator) => {
      if(e.schema.map(_._1).toSet contains CONDITION_COLUMN) {
        List(ProjectArg(CONDITION_COLUMN, Var(CONDITION_COLUMN)))
      } else {
        List[ProjectArg]()
        // List(ProjectArg(CONDITION_COLUMN, BoolPrimitive(true)))
      }
    }
    val afterDescent =
      o.rebuild(
        o.children.map( percolateOne(_) )
        // post-condition: Only the immediate child
        // of o is uncertainty-generating.
      )
    // println("---\nbefore\n"+o+"\nafter\n"+afterDescent+"\n")
    afterDescent match {
      case t: Table => t
      case Project(cols, p2 @ Project(_, source)) =>
        val bindings = p2.bindings
        // println("---\nrebuilding\n"+o)
        // println("mapping with bindings " + bindings.toString)
        val ret = Project(
          (cols ++ addConstraintCol(p2)).map(
            (x) => ProjectArg(
              x.column,
              Eval.inline(x.input, bindings)
          )),
          source
        )
        // println("---\nrebuilt\n"+ret)
        return ret
      case Project(cols, source) =>
        return Project(cols ++ addConstraintCol(source), source)
      case s @ Select(cond1, Select(cond2, source)) =>
        return Select(Arithmetic(Arith.And,cond1,cond2), source)
      case s @ Select(cond, p @ Project(cols, source)) =>
        // Percolate the projection up through the
        // selection
        if(!CTables.isProbabilistic(p)){
          return Project(cols,
            percolateOne(Select(Eval.inline(cond, p.bindings),source))
          )
        } else {
          val newCond = Eval.inline(cond, p.bindings)
          CTables.extractProbabilisticClauses(newCond) match {
            case (BoolPrimitive(true), BoolPrimitive(true)) =>
              // Select clause is irrelevant
              return p
            case (BoolPrimitive(true), c:Expression) =>
              // Select clause is deterministic
              return Project(cols, Select(c, source))
            case (u, c) =>
              // Select clause is at least partly
              // nondeterministic
              val newSelect =
                if(c == BoolPrimitive(true)){ source }
                else { percolateOne(Select(c, source)) }
              val inputCondition =
                cols.find( _.column == CONDITION_COLUMN )
              if(inputCondition.isEmpty){
                return Project(cols ++ List(
                          ProjectArg(CONDITION_COLUMN, u)
                        ),
                        newSelect
                )
              } else {
                return Project(cols.map(
                    (x) => if(x.column == CONDITION_COLUMN){
                      ProjectArg(CONDITION_COLUMN,
                        Arith.makeAnd(x.input, u)
                      )
                    } else { x }
                ), newSelect)
              }
          }
        }
      case s: Select => return s
      case Join(lhs, rhs) => {

        // println("Percolating Join: \n" + o)
        val makeName = (name:String, x:Integer) =>
          (name+"_"+x)
        val (lhsCols, lhsChild) = extractProject(percolate(lhs))
        val (rhsCols, rhsChild) = extractProject(percolate(rhs))

//         println("Percolated LHS: "+lhsCols+"\n" + lhsChild)
//         println("Percolated RHS: "+rhsCols+"\n" + rhsChild)

        // Pulling projections up through a join may require
        // renaming columns under the join if the same column
        // name appears on both sides of the source
        val lhsColNames = lhsChild.schema.map(_._1).toSet
        val rhsColNames = rhsChild.schema.map(_._1).toSet

        val conflicts = (
          (lhsColNames & rhsColNames)
          | (Set[String](ROWID_KEY) & lhsColNames)
          | (Set[String](ROWID_KEY) & rhsColNames)
        )
        var allNames = lhsColNames ++ rhsColNames

        val nameMap = conflicts.map( (col) => {
          var lhsSuffix = 1
          while(allNames.contains(makeName(col, lhsSuffix))){ lhsSuffix += 1; }
          var rhsSuffix = lhsSuffix + 1
          while(allNames.contains(makeName(col, rhsSuffix))){ rhsSuffix += 1; }
          (col, (lhsSuffix, rhsSuffix))
        }).toMap
        val renameLHS = (name:String) =>
          makeName(name, nameMap(name)._1)
        val renameRHS = (name:String) =>
          makeName(name, nameMap(name)._2)

//        println("CONFLICTS: "+conflicts+"in: "+lhsColNames+", "+rhsColNames+"; for \n"+afterDescent);

        val newJoin =
          if(conflicts.isEmpty) {
            Join(lhsChild, rhsChild)
          } else {
            val fullMapping = (name:String, rename:String => String) => {
              ( if(conflicts contains name){ rename(name) }
                else { name },
                Var(name)
              )
            }
            // Create a projection that remaps the names of
            // all the variables to the appropriate unqiue
            // name.
            val rewrite = (child:Operator, rename:String => String) => {
              Project(
                child.schema.map(_._1).
                  map( fullMapping(_, rename) ).
                  map( (x) => ProjectArg(x._1, x._2)).toList,
                child
              )
            }
            Join(
              rewrite(lhsChild, renameLHS),
              rewrite(rhsChild, renameRHS)
            )
          }
        val remap = (cols: List[(String,Expression)], 
                     rename:String => String) =>
        {
          val mapping =
            conflicts.map(
              (x) => (x, Var(rename(x)))
            ).toMap[String, Expression]
          cols.filter( _._1 != CONDITION_COLUMN ).
            map( _ match { case (name, expr) =>
              (name, Eval.inline(expr, mapping))
            })
        }
        var cols = remap(lhsCols, renameLHS) ++
                   remap(rhsCols, renameRHS)
        val lhsHasCondition =
          lhsCols.exists( _._1 == CONDITION_COLUMN)
        val rhsHasCondition =
          rhsCols.exists( _._1 == CONDITION_COLUMN)
        if(lhsHasCondition || rhsHasCondition) {
          if(conflicts contains CONDITION_COLUMN){
            cols = cols ++ List(
              ( CONDITION_COLUMN,
                Arithmetic(Arith.And,
                  Var(renameLHS(CONDITION_COLUMN)),
                  Var(renameRHS(CONDITION_COLUMN))
              )))
          } else {
            cols = cols ++ List(
              (CONDITION_COLUMN, Var(CONDITION_COLUMN))
            )
          }
        }
        // println(cols.toString);
        val ret = {
          if(cols.exists(
              _ match {
                case (colName, Var(varName)) =>
                        (colName != varName)
                case _ => true
              })
            )
          {
            Project( cols.map(
              _ match { case (name, colExpr) =>
                ProjectArg(name, colExpr)
              }),
              newJoin
            )
          } else {
            newJoin
          }
        }
        return ret
      }
    }
  }
}