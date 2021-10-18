def genCode(ctx: CodegenContext): ExprCode = {
  ctx.subExprEliminationExprs.get(this).map { subExprState =>
    // This expression is repeated which means that the code to evaluate it has already been added
    // as a function before. In that case, we just re-use it.
    ExprCode(ctx.registerComment(this.toString), subExprState.isNull, subExprState.value)
  }.getOrElse {
    val isNull = ctx.freshName("isNull")
    val value = ctx.freshName("value")
    val eval = doGenCode(ctx, ExprCode(
      JavaCode.isNullVariable(isNull),
      JavaCode.variable(value, dataType)))
    reduceCodeSize(ctx, eval)
    if (eval.code.toString.nonEmpty) {
      // Add `this` in the comment.
      eval.copy(code = ctx.registerComment(this.toString) + eval.code)
    } else {
      eval
    }
  }
}
