package org.apache.flink.connector.jdbc.internal.converter;

import io.vertx.core.json.JsonArray;

import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Converter that is responsible to convert between JDBC object and Flink SQL internal data
 * structure {@link RowData}.
 */
public interface JdbcRowConverter extends Serializable {

    /**
     * Convert data retrieved from {@link ResultSet} to internal {@link RowData}.
     *
     * @param resultSet ResultSet from JDBC
     */
    RowData toInternal(ResultSet resultSet) throws SQLException;

    /**
     * Convert data retrieved from {@link ResultSet} to internal {@link RowData}.
     *
     * @param jsonArray JsonArray from JDBC
     */
    RowData toInternal(JsonArray jsonArray) throws SQLException;

    /**
     * Convert data retrieved from Flink internal RowData to JDBC Object.
     *
     * @param rowData The given internal {@link RowData}.
     * @param statement The statement to be filled.
     * @return The filled statement.
     */
    FieldNamedPreparedStatement toExternal(RowData rowData, FieldNamedPreparedStatement statement)
            throws SQLException;
}
