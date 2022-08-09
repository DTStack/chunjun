package com.dtstack.chunjun.connector.postgresql.source;

import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormat;

import java.sql.ResultSet;

public class PostgresqlInputFormat extends JdbcInputFormat {

    @Override
    protected int getResultSetType() {
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }
}
