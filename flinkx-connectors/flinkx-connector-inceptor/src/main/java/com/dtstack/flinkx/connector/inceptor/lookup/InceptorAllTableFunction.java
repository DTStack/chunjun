package com.dtstack.flinkx.connector.inceptor.lookup;

import com.dtstack.flinkx.connector.inceptor.conf.InceptorConf;
import com.dtstack.flinkx.connector.inceptor.util.InceptorDbUtil;
import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.lookup.JdbcAllTableFunction;
import com.dtstack.flinkx.lookup.conf.LookupConf;

import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author dujie @Description TODO
 * @createTime 2022-01-20 04:28:00
 */
public class InceptorAllTableFunction extends JdbcAllTableFunction {
    private static final Logger LOG = LoggerFactory.getLogger(InceptorAllTableFunction.class);

    private final InceptorConf inceptorConf;

    public InceptorAllTableFunction(
            InceptorConf inceptorConf,
            JdbcDialect jdbcDialect,
            LookupConf lookupConf,
            String[] fieldNames,
            String[] keyNames,
            RowType rowType) {
        super(inceptorConf, jdbcDialect, lookupConf, fieldNames, keyNames, rowType);
        this.inceptorConf = inceptorConf;
    }

    @Override
    protected void loadData(Object cacheRef) {
        Map<String, List<Map<String, Object>>> tmpCache =
                (Map<String, List<Map<String, Object>>>) cacheRef;
        Connection connection = null;

        try {
            connection = InceptorDbUtil.getConnection(inceptorConf, null, null);
            queryAndFillData(tmpCache, connection);
        } catch (Exception e) {
            LOG.error("", e);
            throw new RuntimeException(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOG.error("", e);
                }
            }
        }
    }
}
