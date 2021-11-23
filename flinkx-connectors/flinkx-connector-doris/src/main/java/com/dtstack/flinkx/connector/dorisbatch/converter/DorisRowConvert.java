package com.dtstack.flinkx.connector.dorisbatch.converter;

import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.StringJoiner;

/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2021-11-21
 */
public class DorisRowConvert
        extends AbstractRowConverter<RowData, RowData, StringJoiner, LogicalType> {

    private static final long serialVersionUID = 2L;

    private static final String NULL_VALUE = "\\N";

    public DorisRowConvert(RowType rowType) {
        super(rowType);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]));
        }
    }

    @Override
    public RowData toInternal(RowData input) throws Exception {
        return null;
    }

    @Override
    public StringJoiner toExternal(RowData rowData, StringJoiner joiner) throws Exception {
        for (int index = 0; index < fieldTypes.length; index++) {
            toExternalConverters.get(index).serialize(rowData, index, joiner);
        }
        return joiner;
    }

    @Override
    protected ISerializationConverter<StringJoiner> wrapIntoNullableExternalConverter(
            ISerializationConverter<StringJoiner> ISerializationConverter, LogicalType type) {
        return ((rowData, index, joiner) -> {
            if (rowData == null
                    || rowData.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                joiner.add(NULL_VALUE);
            } else {
                ISerializationConverter.serialize(rowData, index, joiner);
            }
        });
    }

    @Override
    protected ISerializationConverter<StringJoiner> createExternalConverter(LogicalType type) {
        return (rowData, index, joiner) -> {
            Object value = ((GenericRowData) rowData).getField(index);
            joiner.add("".equals(value.toString()) ? NULL_VALUE : value.toString());
        };
    }
}
