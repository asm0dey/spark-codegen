import kotlin.Pair;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.execution.BufferedRowIterator;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.Iterator;

import java.io.IOException;

public Object generate(Object[]references){return new GeneratedIteratorForCodegenStage1(references);}

// codegenStageId=1
final class GeneratedIteratorForCodegenStage1 extends BufferedRowIterator {
    private Object[] references;
    private Iterator[] inputs;
    private Iterator localtablescan_input_0;
    private int deserializetoobject_argValue_0;
    private String[] deserializetoobject_mutableStateArray_0 = new String[2];
    private UnsafeRowWriter[] deserializetoobject_mutableStateArray_2 = new UnsafeRowWriter[5];
    private Integer[] deserializetoobject_mutableStateArray_1 = new Integer[1];
    private Pair[] mapelements_mutableStateArray_0 = new Pair[1];

    public GeneratedIteratorForCodegenStage1(Object[] references) {
        this.references = references;
    }

    public void init(int index, Iterator[] inputs) {
        partitionIndex = index;
        this.inputs = inputs;
        localtablescan_input_0 = inputs[0];

        deserializetoobject_mutableStateArray_2[0] = new UnsafeRowWriter(1, 32);
        deserializetoobject_mutableStateArray_2[1] = new UnsafeRowWriter(1, 32);
        deserializetoobject_mutableStateArray_2[2] = new UnsafeRowWriter(1, 32);
        deserializetoobject_mutableStateArray_2[3] = new UnsafeRowWriter(1, 32);
        deserializetoobject_mutableStateArray_2[4] = new UnsafeRowWriter(2, 32);

    }

    protected void processNext() throws IOException {
        while (localtablescan_input_0.hasNext()) {
            InternalRow localtablescan_row_0 = (InternalRow) localtablescan_input_0.next();
            ((SQLMetric) references[0] /* numOutputRows */).add(1);
            UTF8String localtablescan_value_0 = localtablescan_row_0.getUTF8String(0);
            int localtablescan_value_1 = localtablescan_row_0.getInt(1);

            deserializetoobject_doConsume_0(localtablescan_row_0, localtablescan_value_0, localtablescan_value_1);
            if (shouldStop()) return;
        }
    }

    private void deserializetoobject_doConsume_0(InternalRow localtablescan_row_0,
                                                 UTF8String deserializetoobject_expr_0_0,
                                                 int deserializetoobject_expr_1_0) throws IOException {
        boolean deserializetoobject_isNull_1 = true;
        String deserializetoobject_value_1 = null;
        if (!false) {
            deserializetoobject_isNull_1 = false;
            if (!deserializetoobject_isNull_1) {
                Object deserializetoobject_funcResult_0 = null;
                deserializetoobject_funcResult_0 = deserializetoobject_expr_0_0.toString();
                deserializetoobject_value_1 = (String) deserializetoobject_funcResult_0;

            }
        }
        deserializetoobject_mutableStateArray_0[0] = deserializetoobject_value_1;

        deserializetoobject_argValue_0 = deserializetoobject_expr_1_0;

        Integer deserializetoobject_value_3 = null;
        if (!false) {
            deserializetoobject_value_3 = Integer.valueOf(deserializetoobject_argValue_0);
        }
        deserializetoobject_mutableStateArray_1[0] = deserializetoobject_value_3;

        final Pair deserializetoobject_value_0 = false ?
                null : new Pair(deserializetoobject_mutableStateArray_0[0], deserializetoobject_mutableStateArray_1[0]);

        mapelements_doConsume_0(deserializetoobject_value_0, false);

    }


    private void mapelements_doConsume_0(Pair mapelements_expr_0_0, boolean mapelements_exprIsNull_0_0) throws IOException {
        boolean mapelements_isNull_1 = true;
        Pair mapelements_value_1 = null;
        if (!false) {
            mapelements_mutableStateArray_0[0] = mapelements_expr_0_0;

            mapelements_isNull_1 = false;
            if (!mapelements_isNull_1) {
                Object mapelements_funcResult_0 = null;

                try {
                    mapelements_funcResult_0 = ((MapFunction) references[1] /* literal */).call(mapelements_mutableStateArray_0[0]);
                } catch (Exception e) {
                    Platform.throwException(e);
                }

                if (mapelements_funcResult_0 != null) {
                    mapelements_value_1 = (Pair) mapelements_funcResult_0;
                } else {
                    mapelements_isNull_1 = true;
                }

            }
        }

        serializefromobject_doConsume_0(mapelements_value_1, mapelements_isNull_1);

    }

    private void serializefromobject_doConsume_0(Pair serializefromobject_expr_0_0,
                                                 boolean serializefromobject_exprIsNull_0_0) throws IOException {
        if (serializefromobject_exprIsNull_0_0) {
            throw new NullPointerException(((String) references[2] /* errMsg */));
        }
        boolean serializefromobject_isNull_1 = true;
        int serializefromobject_value_1 = -1;
        if (!false) {
            serializefromobject_isNull_1 = false;
            if (!serializefromobject_isNull_1) {
                Object serializefromobject_funcResult_0 = null;
                serializefromobject_funcResult_0 = serializefromobject_expr_0_0.getFirst();
                serializefromobject_value_1 = (Integer) serializefromobject_funcResult_0;

            }
        }
        if (serializefromobject_exprIsNull_0_0) {
            throw new NullPointerException(((String) references[3] /* errMsg */));
        }
        boolean serializefromobject_isNull_5 = true;
        String serializefromobject_value_5 = null;
        if (!false) {
            serializefromobject_isNull_5 = false;
            if (!serializefromobject_isNull_5) {
                Object serializefromobject_funcResult_1 = null;
                serializefromobject_funcResult_1 = serializefromobject_expr_0_0.getSecond();
                serializefromobject_value_5 = (String) serializefromobject_funcResult_1;

            }
        }
        deserializetoobject_mutableStateArray_0[1] = serializefromobject_value_5;

        UTF8String serializefromobject_value_4 = null;
        if (!false) {
            serializefromobject_value_4 = UTF8String.fromString(deserializetoobject_mutableStateArray_0[1]);
        }
        deserializetoobject_mutableStateArray_2[4].reset();

        deserializetoobject_mutableStateArray_2[4].zeroOutNullBytes();

        deserializetoobject_mutableStateArray_2[4].write(0, serializefromobject_value_1);

        deserializetoobject_mutableStateArray_2[4].write(1, serializefromobject_value_4);
        append((deserializetoobject_mutableStateArray_2[4].getRow()));
    }
}