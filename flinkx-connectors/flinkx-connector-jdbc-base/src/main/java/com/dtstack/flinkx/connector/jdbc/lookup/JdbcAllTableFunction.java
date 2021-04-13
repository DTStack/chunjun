package com.dtstack.flinkx.connector.jdbc.lookup;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.connector.jdbc.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.converter.AbstractJdbcRowConverter;
import com.dtstack.flinkx.lookup.AbstractAllTableFunction;
import com.dtstack.flinkx.lookup.conf.LookupConf;
import com.dtstack.flinkx.util.DtStringUtil;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
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
public class JdbcAllTableFunction extends AbstractAllTableFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(JdbcAllTableFunction.class);

    private final JdbcConf jdbcConf;
    private final String query;
    private final AbstractJdbcRowConverter jdbcRowConverter;
    private final JdbcDialect jdbcDialect;

    public JdbcAllTableFunction(
            JdbcConf jdbcConf,
            JdbcDialect jdbcDialect,
            LookupConf lookupConf,
            String[] fieldNames,
            String[] keyNames,
            RowType rowType) {
        super(fieldNames, keyNames, lookupConf);
        this.jdbcConf = jdbcConf;
        this.query = jdbcDialect.getSelectFromStatement(
                jdbcConf.getTable(),
                fieldNames,
                new String[]{});
        this.jdbcDialect = jdbcDialect;
        this.jdbcRowConverter = jdbcDialect.getRowConverter(rowType);
    }

    @Override
    protected void loadData(Object cacheRef) {
        Map<String, List<Map<String, Object>>> tmpCache = (Map<String, List<Map<String, Object>>>) cacheRef;
        Connection connection = null;

        try {
            for (int i = 0; i < lookupConf.getMaxRetryTimes(); i++) {
                try {
                    connection = getConn(
                            jdbcConf.getJdbcUrl(),
                            jdbcConf.getUsername(),
                            jdbcConf.getPassword());
                    break;
                } catch (Exception e) {
                    if (i == lookupConf.getMaxRetryTimes() - 1) {
                        throw new RuntimeException("", e);
                    }
                    try {
                        String connInfo = "url:" + jdbcConf.getJdbcUrl() + ";userName:"
                                + jdbcConf.getUsername() + ",pwd:"
                                + jdbcConf.getPassword();
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
        statement.setFetchSize(lookupConf.getFetchSize());
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
    protected Connection getConn(String dbUrl, String userName, String password) {
        try {
            Class.forName(jdbcDialect.defaultDriverName().get());
            //add param useCursorFetch=true
            Map<String, String> addParams = Maps.newHashMap();
            addParams.put("useCursorFetch", "true");
            String targetDbUrl = DtStringUtil.addJdbcParam(dbUrl, addParams, true);
            return DriverManager.getConnection(targetDbUrl, userName, password);
        } catch (Exception e) {
            throw new RuntimeException("", e);
        }
    }
}
