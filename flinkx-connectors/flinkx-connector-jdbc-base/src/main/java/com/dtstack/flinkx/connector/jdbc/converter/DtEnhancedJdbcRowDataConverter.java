package com.dtstack.flinkx.connector.jdbc.converter;

import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/11
 **/
public class DtEnhancedJdbcRowDataConverter {

    protected final RowType rowType;
    protected SampleJdbcRowConverter flinkStdRowConverter;
    protected GenricRowDataConverterWithoutLogicType genricRowDataConverterrWithoutLogicType;


    public DtEnhancedJdbcRowDataConverter(RowType rowType) {
        this.rowType = rowType;
        this.flinkStdRowConverter = new SampleJdbcRowConverter(rowType);
        this.genricRowDataConverterrWithoutLogicType = new GenricRowDataConverterWithoutLogicType();
    }

    public FieldNamedPreparedStatement toExternal(
            RowData rowData, FieldNamedPreparedStatement statement) throws Exception {
        if (rowType != null) {
            // 有LogicType就走精准模式
            flinkStdRowConverter.toExternal(rowData, statement);
        } else if (rowData instanceof GenericRowData) {
            // 没有LogicType 走 instanceof模式
            genricRowDataConverterrWithoutLogicType.toExternal(rowData, statement);
        } else {
            // 没有LogicType还不是GenricRowData。则报错
            throw new Exception("");
        }

        return statement;
    }

}
