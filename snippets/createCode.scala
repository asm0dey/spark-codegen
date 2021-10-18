def createCode(
    ctx: CodegenContext,
    expressions: Seq[Expression],
    useSubexprElimination: Boolean = false): ExprCode = {
  val exprEvals = ctx.generateExpressions(expressions, useSubexprElimination)
  val exprSchemas = expressions.map(e => Schema(e.dataType, e.nullable))

  val numVarLenFields = exprSchemas.count {
    case Schema(dt, _) => !UnsafeRow.isFixedLength(dt)
    // TODO: consider large decimal and interval type
  }

  val rowWriterClass = classOf[UnsafeRowWriter].getName
  val rowWriter = ctx.addMutableState(rowWriterClass, "rowWriter",
    v => s"$v = new $rowWriterClass(${expressions.length}, ${numVarLenFields * 32});")

  // Evaluate all the subexpression.
  val evalSubexpr = ctx.subexprFunctionsCode

  val writeExpressions = writeExpressionsToBuffer(
    ctx, ctx.INPUT_ROW, exprEvals, exprSchemas, rowWriter, isTopLevel = true)

  val code =
    code"""
        |$rowWriter.reset();
        |$evalSubexpr
        |$writeExpressions
      """.stripMargin
  // `rowWriter` is declared as a class field, so we can access it directly in methods.
  ExprCode(code, FalseLiteral, JavaCode.expression(s"$rowWriter.getRow()", classOf[UnsafeRow]))
}
