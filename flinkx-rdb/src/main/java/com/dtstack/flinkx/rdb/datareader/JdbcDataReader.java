/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.rdb.datareader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormatBuilder;
import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.rdb.util.DBUtil;
import com.dtstack.flinkx.reader.DataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ClassUtil;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The Reader plugin for any database that can be connected via JDBC.
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class JdbcDataReader extends DataReader {

    protected DatabaseInterface databaseInterface;

    protected TypeConverterInterface typeConverter;

    protected String dbUrl;

    protected String username;

    protected String password;

    protected List<MetaColumn> metaColumns;

    protected String[] columnTypes;

    protected String table;

    protected Connection connection;

    protected String where;

    protected String splitKey;

    protected int fetchSize;

    protected int queryTimeOut;

    public void setDatabaseInterface(DatabaseInterface databaseInterface) {
        this.databaseInterface = databaseInterface;
    }

    public void setTypeConverterInterface(TypeConverterInterface typeConverter) {
        this.typeConverter = typeConverter;
    }

    public JdbcDataReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        dbUrl = readerConfig.getParameter().getConnection().get(0).getJdbcUrl().get(0);
        dbUrl = DBUtil.formatJdbcUrl(readerConfig.getName(),dbUrl);
        username = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_USER_NAME);
        password = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_PASSWORD);
        table = readerConfig.getParameter().getConnection().get(0).getTable().get(0);
        where = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_WHERE);
        metaColumns = MetaColumn.getMetaColumns(readerConfig.getParameter().getColumn());
        fetchSize = readerConfig.getParameter().getIntVal(JdbcConfigKeys.KEY_FETCH_SIZE,0);
        queryTimeOut = readerConfig.getParameter().getIntVal(JdbcConfigKeys.KEY_QUERY_TIME_OUT,0);
        splitKey = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_SPLIK_KEY);
    }

    @Override
    public DataStream<Row> readData() {
        // Read from JDBC
        JdbcInputFormatBuilder builder = new JdbcInputFormatBuilder();
        builder.setDrivername(databaseInterface.getDriverClass());
        builder.setDBUrl(dbUrl);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setBytes(bytes);
        builder.setMonitorUrls(monitorUrls);
        builder.setTable(table);
        builder.setDatabaseInterface(databaseInterface);
        builder.setTypeConverter(typeConverter);
        builder.setMetaColumn(metaColumns);
        builder.setFetchSize(fetchSize == 0 ? databaseInterface.getFetchSize() : fetchSize);
        builder.setQueryTimeOut(queryTimeOut == 0 ? databaseInterface.getQueryTimeout() : queryTimeOut);

        boolean isSplitByKey = false;
        if(numPartitions > 1 && splitKey != null && splitKey.trim().length() != 0) {
            builder.setParameterValues(DBUtil.getParameterValues(numPartitions));
            isSplitByKey = true;
        }

        String query = DBUtil.getQuerySql(databaseInterface,table,metaColumns,splitKey,where,isSplitByKey);
        builder.setQuery(query);

        RichInputFormat format =  builder.finish();
        return createInput(format, (databaseInterface.getDatabaseType() + "reader").toLowerCase());
    }


    /**
     * FIXME 不通过该方法获取表的字段属性
     * 暂时保留
     * @return
     */
    private RowTypeInfo getColumnTypes() {
        List<String> columnNames = metaColumns.stream().map(MetaColumn::getName).collect(Collectors.toList());
        String sql = databaseInterface.getSQLQueryColumnFields(columnNames, table);

        try (Connection conn = getConnection()) {
            Statement stmt = conn.createStatement();
            ResultSetMetaData rsmd = stmt.executeQuery(sql).getMetaData();
            int cnt = rsmd.getColumnCount();
            TypeInformation[] typeInformations = new TypeInformation[cnt];
            for(int i = 0; i < cnt; ++i) {
                Class<?> clz = Class.forName(rsmd.getColumnClassName(i + 1));
                if(clz == Timestamp.class) {
                    clz = java.util.Date.class;
                }
                typeInformations[i] = BasicTypeInfo.getInfoFor(clz);
            }
            return new RowTypeInfo(typeInformations);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    protected RowTypeInfo getColumnTypesBySetting() {
        try{
            int cnt = columnTypes.length;
            TypeInformation[] typeInformations = new TypeInformation[cnt];
            for(int i = 0; i < cnt; ++i) {
                Class<?> clz = Class.forName(columnTypes[i]);
                if(clz == Timestamp.class || clz == Date.class || clz == Time.class || columnTypes[i].toUpperCase().contains("TIMESTAMP")) {
                    clz = java.util.Date.class;
                }
                typeInformations[i] = BasicTypeInfo.getInfoFor(clz);
            }
            return new RowTypeInfo(typeInformations);
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    protected Connection getConnection() {
        try {
            ClassUtil.forName(databaseInterface.getDriverClass(), this.getClass().getClassLoader());
            connection = DBUtil.getConnection(dbUrl, username, password);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        return connection;

    }
}
