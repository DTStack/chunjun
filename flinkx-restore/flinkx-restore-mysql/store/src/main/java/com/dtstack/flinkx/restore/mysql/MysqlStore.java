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
                    + "(database_name, table_name, operation_type, lsn, content, update_time, status)"
                    + " VALUE ($database_name, $table_name, '$operation_type', '$lsn', '$content', $update_time, 0)";

    private static final String DRIVER = "com.mysql.jdbc.Driver";

    private final StoreConf conf;

    private DataSource dataSource;

    private Connection connection;

    private PreparedStatement preparedStatement;

    private String ddlDatabase;

    private String ddlTable;

    public MysqlStore(StoreConf conf) {
        this.conf = conf;
    }

    @Override
    public void store(RowData data) {
        // 有数据写入了，但是需要记录已经保存数据的表名
        if (data instanceof DdlRowData) {
            DdlRowData ddlRowData = (DdlRowData) data;
            String tableIdentifier = ddlRowData.getTableIdentifier();
            String[] split = tableIdentifier.split("\\.");
            String databaseName = split[0];
            String tableName = split[1];
            String operationType = ddlRowData.getType().getValue();
            String lsn = ddlRowData.getLsn();
            String sql = ddlRowData.getSql();
            String insert =
                    INSERT.replace("$database_name", databaseName)
                            .replace("$table_name", tableName)
                            .replace("$operation_type", operationType)
                            .replace("$lsn", lsn)
                            .replace("$content", sql)
                            .replace("$update_time", "CURRENT_TIMESTAMP")
                            .replace("$database", ddlDatabase)
                            .replace("$table", ddlTable);

            try {
                preparedStatement.execute(insert);
            } catch (SQLException e) {
                throw new RuntimeException("Insert ddl failed! value: " + insert, e);
            }
        }
    }

    @Override
    public void open() throws Exception {
        dataSource = DruidDataSourceUtil.getDataSource(conf.getProperties(), DRIVER);
        connection = dataSource.getConnection();

        this.ddlDatabase = (String) conf.getProperties().get(DATABASE_KEY);
        this.ddlTable = (String) conf.getProperties().get(TABLE_KEY);
        String insert = INSERT.replace("$database", ddlDatabase).replace("$table", ddlTable);
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
