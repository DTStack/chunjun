package com.dtstack.flinkx.connector.jdbc;

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
        return dialect.canHandle(url);
    }
}
