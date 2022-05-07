package com.dtstack.flinkx.connector.jdbc.lookup;

import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.lookup.AbstractAllTableFunction;
import com.dtstack.flinkx.lookup.conf.LookupConf;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
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
    protected final JdbcDialect jdbcDialect;
    private final JdbcConf jdbcConf;
    private final String query;

    public JdbcAllTableFunction(
            JdbcConf jdbcConf,
            JdbcDialect jdbcDialect,
            LookupConf lookupConf,
            String[] fieldNames,
            String[] keyNames,
            RowType rowType) {
        super(fieldNames, keyNames, lookupConf, jdbcDialect.getRowConverter(rowType));
        this.jdbcConf = jdbcConf;
        this.query =
                jdbcDialect.getSelectFromStatement(
                        jdbcConf.getSchema(), jdbcConf.getTable(), fieldNames, new String[] {});
        this.jdbcDialect = jdbcDialect;
    }

    @Override
    protected void loadData(Object cacheRef) {
        Map<String, List<Map<String, Object>>> tmpCache =
                (Map<String, List<Map<String, Object>>>) cacheRef;
        Connection connection = null;

        try {
            connection = JdbcUtil.getConnection(jdbcConf, jdbcDialect);
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
     * @throws SQLException
     */
    protected void queryAndFillData(
            Map<String, List<Map<String, Object>>> tmpCache, Connection connection)
            throws SQLException {
        // load data from table
        Statement statement = connection.createStatement();
        statement.setFetchSize(lookupConf.getFetchSize());
        ResultSet resultSet = statement.executeQuery(query);

        while (resultSet.next()) {
            Map<String, Object> oneRow = new HashMap<>();
            // 防止一条数据有问题，后面数据无法加载
            try {
                GenericRowData rowData = (GenericRowData) rowConverter.toInternal(resultSet);
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
}
