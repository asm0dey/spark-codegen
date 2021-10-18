/**
  * Generates code for expressions. If doSubexpressionElimination is true, subexpression
  * elimination will be performed. Subexpression elimination assumes that the code for each
  * expression will be combined in the `expressions` order.
  */
def generateExpressions(
    expressions: Seq[Expression],
    doSubexpressionElimination: Boolean = false): Seq[ExprCode] = {
  if (doSubexpressionElimination) subexpressionElimination(expressions)
  expressions.map(e => e.genCode(this))
}
