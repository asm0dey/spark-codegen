trait DataTypeWithClass {
  val dt: DataType
  val cls: Class[_]
  val nullable: Boolean
}

trait ComplexWrapper extends DataTypeWithClass

class KDataTypeWrapper(val dt: StructType , val cls: Class[_] , val nullable: Boolean = true)
                    extends StructType with ComplexWrapper {
    // delegate everything
}

case class KComplexTypeWrapper(dt: DataType, cls: Class[_], nullable: Boolean)
                    extends DataType with ComplexWrapper {
    // delegate everything
}

case class KSimpleTypeWrapper(dt: DataType, cls: Class[_], nullable: Boolean)
                    extends DataType with DataTypeWithClass {
    // delegate everything
}

class KStructField(val getterName: String, val delegate: StructField)
                    extends StructField {
    // delegate everything
}
