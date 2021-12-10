package com.dtstack.flinkx.restore.mysql;

import com.dtstack.flinkx.cdc.DdlRowData;
import com.dtstack.flinkx.cdc.store.Fetcher;
import com.dtstack.flinkx.cdc.store.FetcherConf;
import com.dtstack.flinkx.restore.mysql.utils.DruidDataSourceUtil;

import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * TODO by tiezhu: 如果ddl库中存在历史数据，这个时候怎么处理呢？
 *
 * @author tiezhu@dtstack.com
 * @since 2021/12/6 星期一
 */
public class MysqlFetcher extends Fetcher {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String SELECT =
            "select database_name, table_name from `$database`.`$table`"
                    + " where status = 2 and database_name = ? and table_name = ? and lsn = ?";

    private static final String DELETE =
            "delete from `$database`.`$table`"
                    + " where status = 2 and database_name = ? and table_name = ? and lsn = ?";

    private static final String DATABASE_KEY = "database";

    private static final String TABLE_KEY = "table";

    private static final String DRIVER = "com.mysql.jdbc.Driver";

    private final FetcherConf conf;

    private DataSource dataSource;

    private Connection connection;

    private PreparedStatement select;

    private PreparedStatement delete;

    public MysqlFetcher(FetcherConf conf) {
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
                ResultSet resultSet = select.executeQuery();
                return resultSet.next();
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
    public void open() throws Exception {
        dataSource = DruidDataSourceUtil.getDataSource(conf.getProperties(), DRIVER);
        connection = dataSource.getConnection();

        String database = (String) conf.getProperties().get(DATABASE_KEY);
        String table = (String) conf.getProperties().get(TABLE_KEY);
        String select = SELECT.replace("$database", database).replace("$table", table);
        String delete = DELETE.replace("$database", database).replace("$table", table);
        this.select = connection.prepareStatement(select);
        this.delete = connection.prepareStatement(delete);
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
