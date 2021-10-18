private def serializerFor(
                            inputObject: Expression,
                            tpe: `Type`,
                            walkedTypePath: WalkedTypePath,
                            seenTypeSet: Set[`Type`] = Set.empty,
                            predefinedDt: Option[DataTypeWithClass] = None
                            ): Expression = cleanUpReflectionObjects {

    def toCatalystArray(
        input: Expression, 
        elementType: `Type`, 
        predefinedDt: Option[DataTypeWithClass] = None): Expression = {
        predefinedDt.map(_.dt).getOrElse(dataTypeFor(elementType)) match {
        case dt: StructType =>
            val clsName = getClassNameFromType(elementType)
            val newPath = walkedTypePath.recordArray(clsName)
            createSerializerForMapObjects(input, ObjectType(predefinedDt.get.cls),
            serializerFor(_, elementType, newPath, seenTypeSet, predefinedDt))

        case dt: ObjectType =>
            val clsName = getClassNameFromType(elementType)
            val newPath = walkedTypePath.recordArray(clsName)
            createSerializerForMapObjects(input, dt,
            serializerFor(_, elementType, newPath, seenTypeSet))

        case dt@(BooleanType | ByteType | ShortType | IntegerType | LongType |
                    FloatType | DoubleType) =>
            val cls = input.dataType.asInstanceOf[ObjectType].cls
            if (cls.isArray && cls.getComponentType.isPrimitive) {
            createSerializerForPrimitiveArray(input, dt)
            } else {
            createSerializerForGenericArray(
                input, 
                dt, 
                nullable = predefinedDt.map(_.nullable).getOrElse(schemaFor(elementType).nullable)
            )
            }

        case _: StringType =>
            val clsName = getClassNameFromType(typeOf[String])
            val newPath = walkedTypePath.recordArray(clsName)
            createSerializerForMapObjects(input, ObjectType(Class.forName(getClassNameFromType(elementType))),
            serializerFor(_, elementType, newPath, seenTypeSet))


        case dt =>
            createSerializerForGenericArray(
                input, 
                dt, 
                nullable = predefinedDt.map(_.nullable).getOrElse(schemaFor(elementType).nullable)
            )
        }
    }

    baseType(tpe) match {

        //<editor-fold desc="scala-like">
        case _ if !inputObject.dataType.isInstanceOf[ObjectType] && 
            !predefinedDt.exists(_.isInstanceOf[ComplexWrapper]) => inputObject

        case t if isSubtype(t, localTypeOf[Option[_]]) =>
            val TypeRef(_, _, Seq(optType)) = t
            val className = getClassNameFromType(optType)
            val newPath = walkedTypePath.recordOption(className)
            val unwrapped = UnwrapOption(dataTypeFor(optType), inputObject)
            serializerFor(unwrapped, optType, newPath, seenTypeSet)

        // Since List[_] also belongs to localTypeOf[Product], we put this case before
        // "case t if definedByConstructorParams(t)" to make sure it will match to the
        // case "localTypeOf[Seq[_]]"
        case t if isSubtype(t, localTypeOf[Seq[_]]) =>
            val TypeRef(_, _, Seq(elementType)) = t
            toCatalystArray(inputObject, elementType)

        case t if isSubtype(t, localTypeOf[Array[_]]) && predefinedDt.isEmpty =>
            val TypeRef(_, _, Seq(elementType)) = t
            toCatalystArray(inputObject, elementType)

        case t if isSubtype(t, localTypeOf[Map[_, _]]) =>
            val TypeRef(_, _, Seq(keyType, valueType)) = t
            val keyClsName = getClassNameFromType(keyType)
            val valueClsName = getClassNameFromType(valueType)
            val keyPath = walkedTypePath.recordKeyForMap(keyClsName)
            val valuePath = walkedTypePath.recordValueForMap(valueClsName)

            createSerializerForMap(
                inputObject,
                MapElementInformation(
                dataTypeFor(keyType),
                nullable = !keyType.typeSymbol.asClass.isPrimitive,
                serializerFor(_, keyType, keyPath, seenTypeSet)),
                MapElementInformation(
                dataTypeFor(valueType),
                nullable = !valueType.typeSymbol.asClass.isPrimitive,
                serializerFor(_, valueType, valuePath, seenTypeSet))
            )

        case t if isSubtype(t, localTypeOf[scala.collection.Set[_]]) =>
            val TypeRef(_, _, Seq(elementType)) = t

            // There's no corresponding Catalyst type for `Set`, we serialize a `Set` to Catalyst array.
            // Note that the property of `Set` is only kept when manipulating the data as domain object.
            val newInput =
            Invoke(
                inputObject,
                "toSeq",
                ObjectType(classOf[Seq[_]]))

            toCatalystArray(newInput, elementType)

        case t if isSubtype(t, localTypeOf[String]) =>
            createSerializerForString(inputObject)
        case t if isSubtype(t, localTypeOf[java.time.Instant]) =>
            createSerializerForJavaInstant(inputObject)

        case t if isSubtype(t, localTypeOf[java.sql.Timestamp]) =>
            createSerializerForSqlTimestamp(inputObject)

        case t if isSubtype(t, localTypeOf[java.time.LocalDate]) =>
            createSerializerForJavaLocalDate(inputObject)

        case t if isSubtype(t, localTypeOf[java.sql.Date]) => createSerializerForSqlDate(inputObject)

        case t if isSubtype(t, localTypeOf[BigDecimal]) =>
            createSerializerForScalaBigDecimal(inputObject)

        case t if isSubtype(t, localTypeOf[java.math.BigDecimal]) =>
            createSerializerForJavaBigDecimal(inputObject)

        case t if isSubtype(t, localTypeOf[java.math.BigInteger]) =>
            createSerializerForJavaBigInteger(inputObject)

        case t if isSubtype(t, localTypeOf[scala.math.BigInt]) =>
            createSerializerForScalaBigInt(inputObject)

        case t if isSubtype(t, localTypeOf[java.lang.Integer]) =>
            createSerializerForInteger(inputObject)
        case t if isSubtype(t, localTypeOf[Int]) =>
            createSerializerForInteger(inputObject)
        case t if isSubtype(t, localTypeOf[java.lang.Long]) => createSerializerForLong(inputObject)
        case t if isSubtype(t, localTypeOf[Long]) => createSerializerForLong(inputObject)
        case t if isSubtype(t, localTypeOf[java.lang.Double]) => createSerializerForDouble(inputObject)
        case t if isSubtype(t, localTypeOf[Double]) => createSerializerForDouble(inputObject)
        case t if isSubtype(t, localTypeOf[java.lang.Float]) => createSerializerForFloat(inputObject)
        case t if isSubtype(t, localTypeOf[Float]) => createSerializerForFloat(inputObject)
        case t if isSubtype(t, localTypeOf[java.lang.Short]) => createSerializerForShort(inputObject)
        case t if isSubtype(t, localTypeOf[Short]) => createSerializerForShort(inputObject)
        case t if isSubtype(t, localTypeOf[java.lang.Byte]) => createSerializerForByte(inputObject)
        case t if isSubtype(t, localTypeOf[Byte]) => createSerializerForByte(inputObject)
        case t if isSubtype(t, localTypeOf[java.lang.Boolean]) => createSerializerForBoolean(inputObject)
        case t if isSubtype(t, localTypeOf[Boolean]) => createSerializerForBoolean(inputObject)

        case t if isSubtype(t, localTypeOf[java.lang.Enum[_]]) =>
            createSerializerForString(
                Invoke(inputObject, "name", ObjectType(classOf[String]), returnNullable = false))

        case t if t.typeSymbol.annotations.exists(_.tree.tpe =:= typeOf[SQLUserDefinedType]) =>
            val udt = getClassFromType(t)
                .getAnnotation(classOf[SQLUserDefinedType]).udt().getConstructor().newInstance()
            val udtClass = udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()
            createSerializerForUserDefinedType(inputObject, udt, udtClass)

        case t if UDTRegistration.exists(getClassNameFromType(t)) =>
            val udt = UDTRegistration.getUDTFor(getClassNameFromType(t)).get.getConstructor().
                newInstance().asInstanceOf[UserDefinedType[_]]
            val udtClass = udt.getClass
            createSerializerForUserDefinedType(inputObject, udt, udtClass)
        //</editor-fold>

        case _ if predefinedDt.isDefined =>
            predefinedDt.get match {
                case dataType: KDataTypeWrapper =>
                val cls = dataType.cls
                val properties = getJavaBeanReadableProperties(cls)
                val structFields = dataType.dt.fields.map(_.asInstanceOf[KStructField])
                val fields = structFields.map { structField =>
                    val maybeProp = properties.find(it => it.getReadMethod.getName == structField.getterName)
                    if (maybeProp.isEmpty) throw new IllegalArgumentException(s"""Field ${structField.name} is not
                    found among available props, which are: ${properties.map(_.getName).mkString(", ")}""")
                    val fieldName = structField.name
                    val propClass = structField.dataType.asInstanceOf[DataTypeWithClass].cls
                    val propDt = structField.dataType.asInstanceOf[DataTypeWithClass]
                    val fieldValue = Invoke(
                    inputObject,
                    maybeProp.get.getReadMethod.getName,
                    inferExternalType(propClass),
                    returnNullable = propDt.nullable
                    )
                    val newPath = walkedTypePath.recordField(propClass.getName, fieldName)
                    (fieldName, 
                    serializerFor(
                        fieldValue, 
                        getType(propClass), 
                        newPath, 
                        seenTypeSet, if (propDt.isInstanceOf[ComplexWrapper]) Some(propDt) else None)
                    )

                }
                createSerializerForObject(inputObject, fields)

                case otherTypeWrapper: ComplexWrapper =>
                otherTypeWrapper.dt match {
                    case MapType(kt, vt, _) =>
                    val Seq(keyType, valueType) = Seq(kt, vt).map(_.asInstanceOf[DataTypeWithClass].cls).map(getType(_))
                    val Seq(keyDT, valueDT) = Seq(kt, vt).map(_.asInstanceOf[DataTypeWithClass])
                    val keyClsName = getClassNameFromType(keyType)
                    val valueClsName = getClassNameFromType(valueType)
                    val keyPath = walkedTypePath.recordKeyForMap(keyClsName)
                    val valuePath = walkedTypePath.recordValueForMap(valueClsName)

                    createSerializerForMap(
                        inputObject,
                        MapElementInformation(
                        dataTypeFor(keyType),
                        nullable = !keyType.typeSymbol.asClass.isPrimitive,
                        serializerFor(
                            _, 
                            keyType, 
                            keyPath, 
                            seenTypeSet, 
                            Some(keyDT).filter(_.isInstanceOf[ComplexWrapper])
                        )),
                        MapElementInformation(
                        dataTypeFor(valueType),
                        nullable = !valueType.typeSymbol.asClass.isPrimitive,
                        serializerFor(
                            _, 
                            valueType, 
                            valuePath, 
                            seenTypeSet, 
                            Some(valueDT).filter(_.isInstanceOf[ComplexWrapper])
                        ))
                    )
                    case ArrayType(elementType, _) =>
                    toCatalystArray(
                        inputObject, 
                        getType(elementType.asInstanceOf[DataTypeWithClass].cls),
                        Some(elementType.asInstanceOf[DataTypeWithClass])
                    )

                    case StructType(elementType: Array[StructField]) =>
                    val cls = otherTypeWrapper.cls
                    val names = elementType.map(_.name)

                    val beanInfo = Introspector.getBeanInfo(cls)
                    val methods = beanInfo.getMethodDescriptors.filter(it => names.contains(it.getName))


                    val fields = elementType.map { structField =>

                        val maybeProp = methods.find(it => it.getName == structField.name)
                        if (maybeProp.isEmpty) throw new IllegalArgumentException(s"""
                        Field ${structField.name} is not found among available props, 
                        which are: ${methods.map(_.getName).mkString(", ")}""")
                        val fieldName = structField.name
                        val propClass = structField.dataType.asInstanceOf[DataTypeWithClass].cls
                        val propDt = structField.dataType.asInstanceOf[DataTypeWithClass]
                        val fieldValue = Invoke(
                        inputObject,
                        maybeProp.get.getName,
                        inferExternalType(propClass),
                        returnNullable = propDt.nullable
                        )
                        val newPath = walkedTypePath.recordField(propClass.getName, fieldName)
                        (
                            fieldName, 
                            serializerFor(
                                fieldValue, 
                                getType(propClass), 
                                newPath, 
                                seenTypeSet, 
                                if (propDt.isInstanceOf[ComplexWrapper]) Some(propDt) else None
                            )
                        )

                    }
                    createSerializerForObject(inputObject, fields)

                case _ =>
                    throw new UnsupportedOperationException(
                        s"No Encoder found for $tpe\n" + walkedTypePath)

            }
        }

        case t if definedByConstructorParams(t) =>
            if (seenTypeSet.contains(t)) {
                throw new UnsupportedOperationException(
                s"cannot have circular references in class, but got the circular reference of class $t")
            }

            val params = getConstructorParameters(t)
            val fields = params.map { case (fieldName, fieldType) =>
                if (javaKeywords.contains(fieldName)) {
                throw new UnsupportedOperationException(s"`$fieldName` is a reserved keyword and " +
                    "cannot be used as field name\n" + walkedTypePath)
                }

                // SPARK-26730 inputObject won't be null with If's guard below. And KnownNotNul
                // is necessary here. Because for a nullable nested inputObject with struct data
                // type, e.g. StructType(IntegerType, StringType), it will return nullable=true
                // for IntegerType without KnownNotNull. And that's what we do not expect to.
                val fieldValue = Invoke(KnownNotNull(inputObject), fieldName, dataTypeFor(fieldType),
                returnNullable = !fieldType.typeSymbol.asClass.isPrimitive)
                val clsName = getClassNameFromType(fieldType)
                val newPath = walkedTypePath.recordField(clsName, fieldName)
                (fieldName, serializerFor(fieldValue, fieldType, newPath, seenTypeSet + t))
            }
            createSerializerForObject(inputObject, fields)

        case _ =>
            throw new UnsupportedOperationException(
                s"No Encoder found for $tpe\n" + walkedTypePath)
    }
}