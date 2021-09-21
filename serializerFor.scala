def serializerFor(cls: java.lang.Class[_], dt: DataTypeWithClass): Expression = {
    val tpe = getType(cls)
    val clsName = getClassNameFromType(tpe)
    val walkedTypePath = WalkedTypePath().recordRoot(clsName)
    val inputObject = BoundReference(0, ObjectType(cls), nullable = true)
    serializerFor(inputObject, tpe, walkedTypePath, predefinedDt = Some(dt))
}