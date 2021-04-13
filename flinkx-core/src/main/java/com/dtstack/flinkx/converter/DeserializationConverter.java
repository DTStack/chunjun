package com.dtstack.flinkx.converter;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/13
 **/

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.sql.SQLException;

/** Runtime converter to convert field to {@link RowData} type object. */
@FunctionalInterface
public interface DeserializationConverter<T> extends Serializable {
    Object deserialize(T field) throws SQLException;
}
