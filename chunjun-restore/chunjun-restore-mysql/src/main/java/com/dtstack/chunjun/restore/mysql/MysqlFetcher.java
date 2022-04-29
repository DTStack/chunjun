package com.dtstack.chunjun.restore.mysql;

import com.dtstack.chunjun.cdc.DdlRowData;
import com.dtstack.chunjun.cdc.DdlRowDataBuilder;
import com.dtstack.chunjun.cdc.monitor.MonitorConf;
import com.dtstack.chunjun.cdc.monitor.fetch.FetcherBase;
import com.dtstack.chunjun.restore.mysql.utils.DataSourceUtil;

import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/6 星期一
 */
public class MysqlFetcher extends FetcherBase {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final MonitorConf conf;

    private DataSource dataSource;

    private Connection connection;

    private PreparedStatement select;

    private PreparedStatement delete;

    private PreparedStatement query;

    public MysqlFetcher(MonitorConf conf) {
        this.conf = conf;
    }

    @Override
    public boolean fetch(RowData data) {
        if (data instanceof DdlRowData) {
            DdlRowData ddlRowData = (DdlRowData) data;
            String tableIdentifier = ddlRowData.getTableIdentifier();
            String[] split = tableIdentifier.split("\\.");
            try {
                select.setString(1, split[0].replace("'", ""));
                select.setString(2, split[1].replace("'", ""));
                select.setString(3, ddlRowData.getLsn());
                try (ResultSet resultSet = select.executeQuery()) {
                    String table = null;
                    while (resultSet.next()) {
                        table = resultSet.getString(1);
                    }
                    return table != null;
                }
            } catch (SQLException e) {
                throw new RuntimeException(
                        "Select ddl failed! tableIdentifier: " + tableIdentifier, e);
            }
        }

        return false;
    }

    @Override
    public void delete(RowData data) {
        if (data instanceof DdlRowData) {
            DdlRowData ddlRowData = (DdlRowData) data;
            String tableIdentifier = ddlRowData.getTableIdentifier();
            String[] split = tableIdentifier.split("\\.");
            try {
                delete.setString(1, split[0].replace("'", ""));
                delete.setString(2, split[1].replace("'", ""));
                delete.setString(3, ddlRowData.getLsn());
                delete.execute();
            } catch (SQLException e) {
                throw new RuntimeException(
                        "Delete ddl failed! tableIdentifier: " + tableIdentifier, e);
            }
        }
    }

    @Override
    public Map<String, DdlRowData> query() {
        final Map<String, DdlRowData> ddlRowDataMap = new HashMap<>();
        try (final ResultSet resultSet = query.executeQuery()) {
            while (resultSet.next()) {
                String databaseName = resultSet.getString(1);
                String tableName = resultSet.getString(2);
                String operationType = resultSet.getString(3);
                String lsn = resultSet.getString(4);
                String content = resultSet.getString(5);

                DdlRowData ddl =
                        DdlRowDataBuilder.builder()
                                .setDatabaseName(databaseName)
                                .setTableName(tableName)
                                .setType(operationType)
                                .setLsn(lsn)
                                .setContent(content)
                                .build();

                String tableIdentity = "'" + databaseName + "'.'" + tableName + "'";
                ddlRowDataMap.put(tableIdentity, ddl);
            }
        } catch (Exception e) {
            throw new RuntimeException("query database, table failed.", e);
        } finally {
            try {
                query.close();
            } catch (SQLException e) {
                logger.error("close query statement failed.", e);
            }
        }
        return ddlRowDataMap;
    }

    @Override
    public void openSubclass() throws Exception {
        dataSource =
                DataSourceUtil.getDataSource(conf.getProperties(), MysqlFetcherConstant.DRIVER);
        connection = dataSource.getConnection();

        String database = (String) conf.getProperties().get(MysqlFetcherConstant.DATABASE_KEY);
        String table = (String) conf.getProperties().get(MysqlFetcherConstant.TABLE_KEY);

        check(database, table);

        String select =
                MysqlFetcherConstant.SELECT.replace("$database", database).replace("$table", table);
        String delete =
                MysqlFetcherConstant.DELETE.replace("$database", database).replace("$table", table);
        String query =
                MysqlFetcherConstant.QUERY.replace("$database", database).replace("$table", table);
        this.select = connection.prepareStatement(select);
        this.delete = connection.prepareStatement(delete);
        this.query = connection.prepareStatement(query);
    }

    /**
     * check sql privilege.
     *
     * @param database sql database
     * @param table sql table
     * @throws SQLException sql exception
     */
    private void check(String database, String table) throws SQLException {
        try (final Statement statement = connection.createStatement()) {
            StringBuilder checkError = new StringBuilder();
            try {
                statement.execute(
                        MysqlFetcherConstant.SELECT_CHECK
                                .replace("$database", database)
                                .replace("$table", table));
            } catch (Exception e) {
                checkError.append(e.getMessage()).append("\n");
            }

            try {
                statement.execute(
                        MysqlFetcherConstant.DELETE_CHECK
                                .replace("$database", database)
                                .replace("$table", table));
            } catch (Exception e) {
                checkError.append(e.getMessage()).append("\n");
            }

            if (checkError.length() > 0) {
                throw new SQLException(checkError.toString());
            }
        }
    }

    @Override
    public void closeSubclass() {
        try {
            if (null != connection && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            logger.error("close datasource failed! conf: " + conf);
        }

        try {
            if (null != select && !select.isClosed()) {
                select.close();
            }
        } catch (SQLException e) {
            logger.error("close preparedStatement failed! conf: " + conf);
        }

        if (dataSource != null) {
            dataSource = null;
        }
    }
}
