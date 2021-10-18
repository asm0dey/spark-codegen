/**
  * Checks and sets up the state and codegen for subexpression elimination. This finds the
  * common subexpressions, generates the functions that evaluate those expressions and populates
  * the mapping of common subexpressions to the generated functions.
  */
private def subexpressionElimination(expressions: Seq[Expression]): Unit = {
  // Add each expression tree and compute the common subexpressions.
  expressions.foreach(equivalentExpressions.addExprTree(_))

  // Get all the expressions that appear at least twice and set up the state for subexpression
  // elimination.
  val commonExprs = equivalentExpressions.getAllEquivalentExprs.filter(_.size > 1)
  commonExprs.foreach { e =>
    val expr = e.head
    val fnName = freshName("subExpr")
    val isNull = addMutableState(JAVA_BOOLEAN, "subExprIsNull")
    val value = addMutableState(javaType(expr.dataType), "subExprValue")

    // Generate the code for this expression tree and wrap it in a function.
    val eval = expr.genCode(this)
    val fn =
      s"""
          |private void $fnName(InternalRow $INPUT_ROW) {
          |  ${eval.code}
          |  $isNull = ${eval.isNull};
          |  $value = ${eval.value};
          |}
          """.stripMargin

    // Add a state and a mapping of the common subexpressions that are associate with this
    // state. Adding this expression to subExprEliminationExprMap means it will call `fn`
    // when it is code generated. This decision should be a cost based one.
    //
    // The cost of doing subexpression elimination is:
    //   1. Extra function call, although this is probably *good* as the JIT can decide to
    //      inline or not.
    // The benefit doing subexpression elimination is:
    //   1. Running the expression logic. Even for a simple expression, it is likely more than 3
    //      above.
    //   2. Less code.
    // Currently, we will do this for all non-leaf only expression trees (i.e. expr trees with
    // at least two nodes) as the cost of doing it is expected to be low.

    subexprFunctions += s"${addNewFunction(fnName, fn)}($INPUT_ROW);"
    val state = SubExprEliminationState(
      JavaCode.isNullGlobal(isNull),
      JavaCode.global(value, expr.dataType))
    subExprEliminationExprs ++= e.map(_ -> state).toMap
  }
}
