package com.dtstack.flinkx.restore.mysql;

import com.dtstack.flinkx.cdc.DdlRowData;
import com.dtstack.flinkx.cdc.store.Store;
import com.dtstack.flinkx.cdc.store.StoreConf;
import com.dtstack.flinkx.restore.mysql.utils.DruidDataSourceUtil;

import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/6 星期一
 */
public class MysqlStore extends Store {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String DATABASE_KEY = "database";

    private static final String TABLE_KEY = "table";

    private static final String INSERT =
            "INSERT INTO `$database`.`$table` "
                    + "(database_name, table_name, operation_name, lsn, content, update_time, status)"
                    + " VALUE (?, ?, ?, ?, ?, 2)";

    private static final String DRIVER = "com.mysql.jdbc.Driver";

    private final StoreConf conf;

    private DataSource dataSource;

    private Connection connection;

    private PreparedStatement preparedStatement;

    public MysqlStore(StoreConf conf) {
        this.conf = conf;
    }

    @Override
    public void store(RowData data) {
        if (data instanceof DdlRowData) {
            DdlRowData ddlRowData = (DdlRowData) data;
            String tableIdentifier = ddlRowData.getTableIdentifier();
            String[] split = tableIdentifier.split("\\.");
            LinkedList<Object> values = new LinkedList<>();
            values.add(split[0]);
            values.add(split[1]);
            values.add(ddlRowData.getType().getValue());
            values.add(ddlRowData.getLsn());
            values.add(ddlRowData.getSql());
            values.add(System.currentTimeMillis());
            for (int i = 0; i < values.size(); i++) {
                try {
                    preparedStatement.setObject(i + 1, values.get(i));
                } catch (SQLException e) {
                    throw new RuntimeException("Insert ddl failed! value: " + values.get(i), e);
                }
            }
        }
    }

    @Override
    public void open() throws Exception {
        dataSource = DruidDataSourceUtil.getDataSource(conf.getProperties(), DRIVER);
        connection = dataSource.getConnection();

        String database = (String) conf.getProperties().get(DATABASE_KEY);
        String table = (String) conf.getProperties().get(TABLE_KEY);
        String insert = INSERT.replace("$database", database).replace("$table", table);
        preparedStatement = connection.prepareStatement(insert);
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
            if (null != preparedStatement && !preparedStatement.isClosed()) {
                preparedStatement.close();
            }
        } catch (SQLException e) {
            logger.error("close preparedStatement failed! conf: " + conf);
        }

        if (dataSource != null) {
            dataSource = null;
        }
    }
}
