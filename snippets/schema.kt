@OptIn(ExperimentalStdlibApi::class)
fun schema(type: KType, map: Map<String, KType> = mapOf()): DataType {
    val primitiveSchema = knownDataTypes[type.classifier]
    if (primitiveSchema != null) return KSimpleTypeWrapper(primitiveSchema,
        (type.classifier!! as KClass<*>).java,
        type.isMarkedNullable)
    val klass = type.classifier as? KClass<*> ?: 
        throw IllegalArgumentException("Unsupported type $type")
    val args: List<KTypeProjection> = type.arguments

    val types = transitiveMerge(map, klass.typeParameters.zip(args).map {
        it.first.name to it.second.type!!
    }.toMap())
    return when {
        klass.isSubclassOf(Enum::class) -> {
            KSimpleTypeWrapper(DataTypes.StringType, klass.java, type.isMarkedNullable)
        }
        klass.isSubclassOf(Iterable::class) || klass.java.isArray -> {
            val listParam = if (klass.java.isArray) {
                when (klass) {
                    IntArray::class -> typeOf<Int>()
                    LongArray::class -> typeOf<Long>()
                    FloatArray::class -> typeOf<Float>()
                    DoubleArray::class -> typeOf<Double>()
                    BooleanArray::class -> typeOf<Boolean>()
                    ShortArray::class -> typeOf<Short>()
                    ByteArray::class -> typeOf<Byte>()
                    else -> types.getValue(klass.typeParameters[0].name)
                }
            } else types.getValue(klass.typeParameters[0].name)
            KComplexTypeWrapper(
                DataTypes.createArrayType(schema(listParam, types), listParam.isMarkedNullable),
                klass.java,
                listParam.isMarkedNullable
            )
        }
        klass.isSubclassOf(Map::class) -> {
            val mapKeyParam = types.getValue(klass.typeParameters[0].name)
            val mapValueParam = types.getValue(klass.typeParameters[1].name)
            KComplexTypeWrapper(
                DataTypes.createMapType(
                    schema(mapKeyParam, types),
                    schema(mapValueParam, types),
                    true
                ),
                klass.java,
                mapValueParam.isMarkedNullable
            )
        }
        klass.isData -> {
            val structType = StructType(
                klass
                    .primaryConstructor!!
                    .parameters
                    .filter { it.findAnnotation<Transient>() == null }
                    .map {
                        val projectedType = types[it.type.toString()] ?: it.type
                        val propertyDescriptor = PropertyDescriptor(it.name,
                            klass.java,
                            "is" + it.name?.replaceFirstChar {
                                 if (it.isLowerCase()) it.titlecase(Locale.getDefault()) 
                                 else it.toString() 
                            },
                            null)
                        KStructField(propertyDescriptor.readMethod.name,
                            StructField(it.name,
                                schema(projectedType, types),
                                projectedType.isMarkedNullable,
                                Metadata.empty()))
                    }
                    .toTypedArray()
            )
            KDataTypeWrapper(structType, klass.java, true)
        }
        klass.isSubclassOf(Product::class) -> {
            val params = type.arguments.mapIndexed { i, it ->
                "_${i + 1}" to it.type!!
            }

            val structType = DataTypes.createStructType(
                params.map { (fieldName, fieldType) ->
                    val dataType = schema(fieldType, types)
                    KStructField(fieldName,
                        StructField(
                            fieldName, 
                            dataType, 
                            fieldType.isMarkedNullable, Metadata.empty()
                        )
                    )
                }.toTypedArray()
            )

            KComplexTypeWrapper(structType, klass.java, true)
        }
        else -> throw IllegalArgumentException("$type is unsupported")
    }
}