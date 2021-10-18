private def create(
        expressions: Seq[Expression],
        subexpressionEliminationEnabled: Boolean): UnsafeProjection = {
    val ctx = newCodeGenContext()
    val eval = createCode(ctx, expressions, subexpressionEliminationEnabled)

    val codeBody =
        s"""
            |public java.lang.Object generate(Object[] references) {
            |  return new SpecificUnsafeProjection(references);
            |}
            |
            |class SpecificUnsafeProjection extends ${classOf[UnsafeProjection].getName} {
            |
            |  private Object[] references;
            |  ${ctx.declareMutableStates()}
            |
            |  public SpecificUnsafeProjection(Object[] references) {
            |    this.references = references;
            |    ${ctx.initMutableStates()}
            |  }
            |
            |  public void initialize(int partitionIndex) {
            |    ${ctx.initPartition()}
            |  }
            |
            |  // Scala.Function1 need this
            |  public java.lang.Object apply(java.lang.Object row) {
            |    return apply((InternalRow) row);
            |  }
            |
            |  public UnsafeRow apply(InternalRow ${ctx.INPUT_ROW}) {
            |    ${eval.code}
            |    return ${eval.value};
            |  }
            |
            |  ${ctx.declareAddedFunctions()}
            |}
        """.stripMargin

    val code = CodeFormatter.stripOverlappingComments(
        new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"code for ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[UnsafeProjection]
}