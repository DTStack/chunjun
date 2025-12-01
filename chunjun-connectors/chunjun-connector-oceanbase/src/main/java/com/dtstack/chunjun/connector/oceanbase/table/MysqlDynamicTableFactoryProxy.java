package com.dtstack.chunjun.connector.oceanbase.table;

import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.mysql.table.MysqlDynamicTableFactory;
import com.dtstack.chunjun.connector.oceanbase.dialect.OceanbaseMysqlModeDialect;

public class MysqlDynamicTableFactoryProxy extends MysqlDynamicTableFactory {
    @Override
    protected JdbcDialect getDialect() {
        return new OceanbaseMysqlModeDialect();
    }
}
