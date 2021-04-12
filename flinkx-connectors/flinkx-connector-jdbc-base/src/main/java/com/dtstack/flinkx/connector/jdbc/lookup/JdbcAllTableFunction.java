package com.dtstack.flinkx.connector.jdbc.lookup;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.lookup.AbstractAllTableFunction;
import com.dtstack.flinkx.lookup.options.LookupOptions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * A lookup function for {@link }.
 *
 * @author chuixue
 */
@Internal
abstract public class JdbcAllTableFunction extends AbstractAllTableFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(JdbcAllTableFunction.class);

    private final JdbcOptions options;
    private final String query;
    private final JdbcRowConverter jdbcRowConverter;

    public JdbcAllTableFunction(
            JdbcOptions options,
            LookupOptions lookupOptions,
            String[] fieldNames,
            String[] keyNames,
            RowType rowType) {
        super(fieldNames, keyNames, lookupOptions);
        this.options = options;
        this.query =
                options.getDialect()
                        .getSelectFromStatement(options.getTableName(), fieldNames, new String[]{});
        this.jdbcRowConverter = options.getDialect().getRowConverter(rowType);
    }

    @Override
    protected void loadData(Object cacheRef) {
        Map<String, List<Map<String, Object>>> tmpCache = (Map<String, List<Map<String, Object>>>) cacheRef;
        Connection connection = null;

        try {
            for (int i = 0; i < lookupOptions.getMaxRetryTimes(); i++) {
                try {
                    connection = getConn(
                            options.getDbURL(),
                            options.getUsername().get(),
                            options.getPassword().get());
                    break;
                } catch (Exception e) {
                    if (i == lookupOptions.getMaxRetryTimes() - 1) {
                        throw new RuntimeException("", e);
                    }
                    try {
                        String connInfo = "url:" + options.getDbURL() + ";userName:" + options
                                .getUsername()
                                .get() + ",pwd:" + options.getPassword().get();
                        LOG.warn("get conn fail, wait for 5 sec and try again, connInfo:"
                                + connInfo);
                        Thread.sleep(5 * 1000);
                    } catch (InterruptedException e1) {
                        LOG.error("", e1);
                    }
                }
            }
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

    /**
     * fill data
     *
     * @param tmpCache
     * @param connection
     *
     * @throws SQLException
     */
    private void queryAndFillData(
            Map<String, List<Map<String, Object>>> tmpCache,
            Connection connection) throws SQLException {
        //load data from table
        Statement statement = connection.createStatement();
        statement.setFetchSize(lookupOptions.getFetchSize());
        ResultSet resultSet = statement.executeQuery(query);

        while (resultSet.next()) {
            Map<String, Object> oneRow = Maps.newHashMap();
            // 防止一条数据有问题，后面数据无法加载
            try {
                GenericRowData rowData = (GenericRowData) jdbcRowConverter.toInternal(resultSet);
                for (int i = 0; i < fieldsName.length; i++) {
                    Object object = rowData.getField(i);
                    oneRow.put(fieldsName[i].trim(), object);
                }
                buildCache(oneRow, tmpCache);
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
    }

    /**
     * get jdbc connection
     *
     * @param dbUrl
     * @param userName
     * @param password
     *
     * @return
     */
    public abstract Connection getConn(String dbUrl, String userName, String password);
}
