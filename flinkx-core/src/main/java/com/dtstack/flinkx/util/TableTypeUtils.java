package com.dtstack.flinkx.util;

import com.dtstack.flinkx.RawTypeConverter;

import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.sql.SQLException;
import java.util.List;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/15
 **/
public class TableTypeUtils {

    /**
     * only using in data sync/integration
     * @param fieldNames field Names
     * @param types field types
     * @return
     */
    public static LogicalType createRowType(List<String> fieldNames, List<String> types, RawTypeConverter converter) throws SQLException {
        TableSchema.Builder builder = TableSchema.builder();
        for(int i = 0; i < types.size(); i++) {
            DataType dataType = converter.apply(types.get(i));
            builder.add(TableColumn.physical(fieldNames.get(i), dataType));
        }

        return builder
                .build()
                .toRowDataType()
                .getLogicalType();
    }
}
