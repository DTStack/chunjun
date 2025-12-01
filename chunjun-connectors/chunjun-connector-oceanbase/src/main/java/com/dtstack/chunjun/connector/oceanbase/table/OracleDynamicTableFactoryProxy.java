package com.dtstack.chunjun.connector.oceanbase.table;

import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.oceanbase.dialect.OceanbaseOracleModeDialect;
import com.dtstack.chunjun.connector.oracle.table.OracleDynamicTableFactory;

public class OracleDynamicTableFactoryProxy extends OracleDynamicTableFactory {
    @Override
    protected JdbcDialect getDialect() {
        return new OceanbaseOracleModeDialect();
    }
}
