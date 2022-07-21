package com.dtstack.chunjun.connector.mysqlm.source;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.conf.ConnectionConf;
import com.dtstack.chunjun.connector.jdbc.conf.DataSourceConf;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.distribute.DistributedJdbcInputFormat;
import com.dtstack.chunjun.connector.jdbc.source.distribute.DistributedJdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.distribute.DistributedJdbcSourceFactory;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.mysql.dialect.MysqlDialect;
import com.dtstack.chunjun.connector.mysqlm.utils.MySqlDataSource;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MysqlmSourceFactory extends DistributedJdbcSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlmSourceFactory.class);

    private final AtomicInteger jdbcPatternCounter = new AtomicInteger(0);
    private final AtomicInteger jdbcTableCounter = new AtomicInteger(0);

    public MysqlmSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env, new MysqlDialect());
        JdbcUtil.putExtParam(jdbcConf);
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        DistributedJdbcInputFormatBuilder builder =
                new DistributedJdbcInputFormatBuilder(new DistributedJdbcInputFormat());
        List<ConnectionConf> connectionConfList = jdbcConf.getConnection();
        List<DataSourceConf> dataSourceConfList = new ArrayList<>(connectionConfList.size());
        try {
            /** 可以是多个 connection、多个 database、多个 table， 或者 正则匹配的 database、table */
            for (ConnectionConf connectionConf : connectionConfList) {
                String currentUsername =
                        (StringUtils.isNotBlank(connectionConf.getUsername()))
                                ? connectionConf.getUsername()
                                : jdbcConf.getUsername();
                String currentPassword =
                        (StringUtils.isNotBlank(connectionConf.getPassword()))
                                ? connectionConf.getPassword()
                                : jdbcConf.getPassword();
                String jdbcUrl = connectionConf.obtainJdbcUrl();
                Connection connection =
                        MySqlDataSource.getDataSource(jdbcUrl, currentUsername, currentPassword)
                                .getConnection();
                for (String table : connectionConf.getTable()) {
                    dataSourceConfList.addAll(
                            allTables(
                                    connection,
                                    connectionConf.getSchema(),
                                    jdbcUrl,
                                    currentUsername,
                                    currentPassword,
                                    table));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Configuration resolution exception.....");
        }
        builder.setSourceList(dataSourceConfList);
        return builder;
    }

    public List<DataSourceConf> allTables(
            Connection connection,
            String currSchema,
            String url,
            String user,
            String pass,
            String currTable)
            throws SQLException {
        Map<String, List<String>> tables = new HashMap<>();
        List<DataSourceConf> dataSourceConfList = new ArrayList<>();

        Pattern schema = Pattern.compile(currSchema);
        Pattern table = Pattern.compile(currTable);
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet catalogs = metaData.getCatalogs();
        while (catalogs.next()) {
            String schemaName = catalogs.getString(1);
            Matcher matcher = schema.matcher(schemaName);
            if (matcher.matches()) {
                tables.put(schemaName, new ArrayList<>());
            }
        }
        for (String schemaName : tables.keySet()) {
            ResultSet tableResult = metaData.getTables(currSchema, null, null, null);
            while (tableResult.next()) {
                String tableName = tableResult.getString("TABLE_NAME");
                LOG.info(
                        "query table [{}]: {}, table: {}.{}",
                        jdbcTableCounter.incrementAndGet(),
                        url,
                        schema,
                        tableName);
                Matcher matcher = table.matcher(tableName);
                if (matcher.matches()) {
                    tables.get(schemaName).add(tableName);
                }
            }
        }

        for (String key : tables.keySet()) {
            for (String mTable : tables.get(key)) {
                DataSourceConf dataSourceConf = new DataSourceConf();
                dataSourceConf.setJdbcUrl(url);
                dataSourceConf.setUserName(user);
                dataSourceConf.setPassword(pass);
                dataSourceConf.setSchema(key);
                dataSourceConf.setTable(mTable);
                LOG.info(
                        "pattern jdbcurl: [{}] {}, table: {}.{}",
                        jdbcPatternCounter.incrementAndGet(),
                        url,
                        key,
                        mTable);
                dataSourceConfList.add(dataSourceConf);
            }
        }
        return dataSourceConfList;
    }
}
