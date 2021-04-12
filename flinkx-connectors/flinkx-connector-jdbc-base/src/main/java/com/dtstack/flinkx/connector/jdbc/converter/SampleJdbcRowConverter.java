package com.dtstack.flinkx.connector.jdbc.converter;

import io.vertx.core.json.JsonArray;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.sql.SQLException;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/12
 **/
public class SampleJdbcRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "MySQL";
    }

    public SampleJdbcRowConverter(RowType rowType) {
        super(rowType);
    }

    //以下不用看
    /**
     * 下不用管
     * @param jsonArray JsonArray from JDBC
     * @return
     * @throws SQLException
     */
    @Override
    public RowData toInternal(JsonArray jsonArray) throws SQLException {
        return null;
    }
}
