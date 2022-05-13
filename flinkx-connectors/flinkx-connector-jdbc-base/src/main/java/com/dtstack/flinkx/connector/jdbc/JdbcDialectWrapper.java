package com.dtstack.flinkx.connector.jdbc;

import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.converter.RawTypeConverter;

/** jdbc 方言. */
public class JdbcDialectWrapper implements JdbcDialect {

    private final org.apache.flink.connector.jdbc.dialect.JdbcDialect dialect;

    public JdbcDialectWrapper(org.apache.flink.connector.jdbc.dialect.JdbcDialect dialect) {
        this.dialect = dialect;
    }

    @Override
    public String dialectName() {
        return dialect.dialectName();
    }

    @Override
    public boolean canHandle(String url) {
        // return dialect.canHandle(url);
        // TODO @wujuan
        return true;
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return null;
    }
}
