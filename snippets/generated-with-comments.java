/* 001 */ public Object generate(Object[] references) {
/* 002 */   return new GeneratedIteratorForCodegenStage1(references);
/* 003 */ }
/* 004 */
/* 005 */ // codegenStageId=1
/* 006 */ final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
/* 007 */   private Object[] references;
/* 008 */   private scala.collection.Iterator[] inputs;
/* 009 */   private scala.collection.Iterator localtablescan_input_0;
/* 010 */   private int deserializetoobject_argValue_0;
/* 011 */   private java.lang.String[] deserializetoobject_mutableStateArray_0 = new java.lang.String[2];
/* 012 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] deserializetoobject_mutableStateArray_2 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[5];
/* 013 */   private java.lang.Integer[] deserializetoobject_mutableStateArray_1 = new java.lang.Integer[1];
/* 014 */   private kotlin.Pair[] mapelements_mutableStateArray_0 = new kotlin.Pair[1];
/* 015 */
/* 016 */   public GeneratedIteratorForCodegenStage1(Object[] references) {
/* 017 */     this.references = references;
/* 018 */   }
/* 019 */
/* 020 */   public void init(int index, scala.collection.Iterator[] inputs) {
/* 021 */     partitionIndex = index;
/* 022 */     this.inputs = inputs;
/* 023 */     localtablescan_input_0 = inputs[0];
/* 024 */
/* 025 */     deserializetoobject_mutableStateArray_2[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);
/* 026 */     deserializetoobject_mutableStateArray_2[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);
/* 027 */     deserializetoobject_mutableStateArray_2[2] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);
/* 028 */     deserializetoobject_mutableStateArray_2[3] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 32);
/* 029 */     deserializetoobject_mutableStateArray_2[4] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 32);
/* 030 */
/* 031 */   }
/* 032 */
/* 033 */   private void mapelements_doConsume_0(kotlin.Pair mapelements_expr_0_0, boolean mapelements_exprIsNull_0_0) throws java.io.IOException {
/* 034 */     boolean mapelements_isNull_1 = true;
/* 035 */     kotlin.Pair mapelements_value_1 = null;
/* 036 */     if (!false) {
/* 037 */       mapelements_mutableStateArray_0[0] = mapelements_expr_0_0;
/* 038 */
/* 039 */       mapelements_isNull_1 = false;
/* 040 */       if (!mapelements_isNull_1) {
/* 041 */         Object mapelements_funcResult_0 = null;
/* 042 */
/* 043 */         try {
/* 044 */           mapelements_funcResult_0 = ((org.apache.spark.api.java.function.MapFunction) references[1] /* literal */).call(mapelements_mutableStateArray_0[0]);
/* 045 */         } catch (Exception e) {
/* 046 */           org.apache.spark.unsafe.Platform.throwException(e);
/* 047 */         }
/* 048 */
/* 049 */         if (mapelements_funcResult_0 != null) {
/* 050 */           mapelements_value_1 = (kotlin.Pair) mapelements_funcResult_0;
/* 051 */         } else {
/* 052 */           mapelements_isNull_1 = true;
/* 053 */         }
/* 054 */
/* 055 */       }
/* 056 */     }
/* 057 */
/* 058 */     serializefromobject_doConsume_0(mapelements_value_1, mapelements_isNull_1);
/* 059 */
/* 060 */   }
/* 061 */
/* 062 */   private void deserializetoobject_doConsume_0(InternalRow localtablescan_row_0, UTF8String deserializetoobject_expr_0_0, int deserializetoobject_expr_1_0) throws java.io.IOException {
/* 063 */     boolean deserializetoobject_isNull_1 = true;
/* 064 */     java.lang.String deserializetoobject_value_1 = null;
/* 065 */     if (!false) {
/* 066 */       deserializetoobject_isNull_1 = false;
/* 067 */       if (!deserializetoobject_isNull_1) {
/* 068 */         Object deserializetoobject_funcResult_0 = null;
/* 069 */         deserializetoobject_funcResult_0 = deserializetoobject_expr_0_0.toString();
/* 070 */         deserializetoobject_value_1 = (java.lang.String) deserializetoobject_funcResult_0;
/* 071 */
/* 072 */       }
/* 073 */     }
/* 074 */     deserializetoobject_mutableStateArray_0[0] = deserializetoobject_value_1;
/* 075 */
/* 076 */     deserializetoobject_argValue_0 = deserializetoobject_expr_1_0;
/* 077 */
/* 078 */     java.lang.Integer deserializetoobject_value_3 = null;
/* 079 */     if (!false) {
/* 080 */       deserializetoobject_value_3 = java.lang.Integer.valueOf(deserializetoobject_argValue_0);
/* 081 */     }
/* 082 */     deserializetoobject_mutableStateArray_1[0] = deserializetoobject_value_3;
/* 083 */
/* 084 */     final kotlin.Pair deserializetoobject_value_0 = false ?
/* 085 */     null : new kotlin.Pair(deserializetoobject_mutableStateArray_0[0], deserializetoobject_mutableStateArray_1[0]);
/* 086 */
/* 087 */     mapelements_doConsume_0(deserializetoobject_value_0, false);
/* 088 */
/* 089 */   }
/* 090 */
/* 091 */   private void serializefromobject_doConsume_0(kotlin.Pair serializefromobject_expr_0_0, boolean serializefromobject_exprIsNull_0_0) throws java.io.IOException {
/* 092 */     if (serializefromobject_exprIsNull_0_0) {
/* 093 */       throw new NullPointerException(((java.lang.String) references[2] /* errMsg */));
/* 094 */     }
/* 095 */     boolean serializefromobject_isNull_1 = true;
/* 096 */     int serializefromobject_value_1 = -1;
/* 097 */     if (!false) {
/* 098 */       serializefromobject_isNull_1 = false;
/* 099 */       if (!serializefromobject_isNull_1) {
/* 100 */         Object serializefromobject_funcResult_0 = null;
/* 101 */         serializefromobject_funcResult_0 = serializefromobject_expr_0_0.getFirst();
/* 102 */         serializefromobject_value_1 = (Integer) serializefromobject_funcResult_0;
/* 103 */
/* 104 */       }
/* 105 */     }
/* 106 */     if (serializefromobject_exprIsNull_0_0) {
/* 107 */       throw new NullPointerException(((java.lang.String) references[3] /* errMsg */));
/* 108 */     }
/* 109 */     boolean serializefromobject_isNull_5 = true;
/* 110 */     java.lang.String serializefromobject_value_5 = null;
/* 111 */     if (!false) {
/* 112 */       serializefromobject_isNull_5 = false;
/* 113 */       if (!serializefromobject_isNull_5) {
/* 114 */         Object serializefromobject_funcResult_1 = null;
/* 115 */         serializefromobject_funcResult_1 = serializefromobject_expr_0_0.getSecond();
/* 116 */         serializefromobject_value_5 = (java.lang.String) serializefromobject_funcResult_1;
/* 117 */
/* 118 */       }
/* 119 */     }
/* 120 */     deserializetoobject_mutableStateArray_0[1] = serializefromobject_value_5;
/* 121 */
/* 122 */     UTF8String serializefromobject_value_4 = null;
/* 123 */     if (!false) {
/* 124 */       serializefromobject_value_4 = org.apache.spark.unsafe.types.UTF8String.fromString(deserializetoobject_mutableStateArray_0[1]);
/* 125 */     }
/* 126 */     deserializetoobject_mutableStateArray_2[4].reset();
/* 127 */
/* 128 */     deserializetoobject_mutableStateArray_2[4].zeroOutNullBytes();
/* 129 */
/* 130 */     deserializetoobject_mutableStateArray_2[4].write(0, serializefromobject_value_1);
/* 131 */
/* 132 */     deserializetoobject_mutableStateArray_2[4].write(1, serializefromobject_value_4);
/* 133 */     append((deserializetoobject_mutableStateArray_2[4].getRow()));
/* 134 */
/* 135 */   }
/* 136 */
/* 137 */   protected void processNext() throws java.io.IOException {
/* 138 */     while ( localtablescan_input_0.hasNext()) {
/* 139 */       InternalRow localtablescan_row_0 = (InternalRow) localtablescan_input_0.next();
/* 140 */       ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
/* 141 */       UTF8String localtablescan_value_0 = localtablescan_row_0.getUTF8String(0);
/* 142 */       int localtablescan_value_1 = localtablescan_row_0.getInt(1);
/* 143 */
/* 144 */       deserializetoobject_doConsume_0(localtablescan_row_0, localtablescan_value_0, localtablescan_value_1);
/* 145 */       if (shouldStop()) return;
/* 146 */     }
/* 147 */   }
/* 148 */
/* 149 */ }
