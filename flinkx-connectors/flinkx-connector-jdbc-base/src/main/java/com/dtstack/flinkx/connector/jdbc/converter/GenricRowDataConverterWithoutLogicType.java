package com.dtstack.flinkx.connector.jdbc.converter;

import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.RowData;

import java.sql.SQLException;

/**
 *
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/12
 **/
// TODO  这个类之后保存在Core模块
public class GenricRowDataConverterWithoutLogicType {

    public FieldNamedPreparedStatement toExternal(
            RowData rowData, FieldNamedPreparedStatement statement) throws SQLException {
        // TODO 实现没有类型的GenricRowData转换
        return statement;
    }
}
