package com.dtstack.flinkx.connector.jdbc;

import com.dtstack.flinkx.RawTypeConverter;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;

import com.dtstack.flinkx.util.TableTypeUtils;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.flink.table.types.logical.LogicalType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/19
 **/
public abstract class JdbcLogicalTypeFactory {

    protected JdbcConf jdbcConf;
    protected JdbcDialect jdbcDialect;

    public JdbcLogicalTypeFactory(JdbcConf jdbcConf, JdbcDialect jdbcDialect) {
        this.jdbcConf = jdbcConf;
        this.jdbcDialect = jdbcDialect;
    }

    public LogicalType createLogicalType() throws SQLException {
        // TODO 从元数据中获取数据类型，如果获取不到元数据信息应该从用户JSON配置中获取
        try (Connection conn = JdbcUtil.getConnection(jdbcConf, jdbcDialect)) {
            Pair<List<String>, List<String>> pair = JdbcUtil.getTableMetaData(jdbcConf.getTable(), conn);
            List<String> rawFieldNames =  pair.getLeft();
            List<String> rawFieldTypes = pair.getRight();
            return TableTypeUtils.createRowType(rawFieldNames, rawFieldTypes, getRawTypeConverter());
        }
    }

    public abstract RawTypeConverter getRawTypeConverter();
}
