package com.dtstack.flinkx;

import org.apache.flink.table.types.DataType;

import java.sql.SQLException;

/**
 * Each conncetor
 */
@FunctionalInterface
public interface RawTypeConverter {

     DataType apply(String type) throws SQLException;

}
