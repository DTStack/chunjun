package com.dtstack.chunjun.connector.oceanbase.dialect;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.connector.oceanbase.converter.OceanbaseOracleSyncConverter;
import com.dtstack.chunjun.connector.oracle.converter.OracleRawTypeConverter;
import com.dtstack.chunjun.connector.oracle.dialect.OracleDialect;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.vertx.core.json.JsonArray;

import java.sql.ResultSet;
import java.util.Optional;

public class OceanbaseOracleModeDialect extends OracleDialect {
    OceanbaseDialect oceanbaseDialect = new OceanbaseDialect();

    @Override
    public String dialectName() {
        return oceanbaseDialect.dialectName();
    }

    @Override
    public boolean canHandle(String url) {
        return oceanbaseDialect.canHandle(url);
    }

    @Override
    public RawTypeMapper getRawTypeConverter() {
        return OracleRawTypeConverter::apply;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return oceanbaseDialect.defaultDriverName();
    }

    @Override
    public AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
            getColumnConverter(RowType rowType, CommonConfig commonConfig) {
        return new OceanbaseOracleSyncConverter(rowType, commonConfig);
    }
}
