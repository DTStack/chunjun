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

package com.dtstack.flinkx.rdb.datawriter;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.rdb.util.DBUtil;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.writer.DataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.types.Row;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static com.dtstack.flinkx.rdb.datawriter.JdbcConfigKeys.*;

/**
 * The Writer plugin for any database that can be connected via JDBC.
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class JdbcDataWriter extends DataWriter {

    protected DatabaseInterface databaseInterface;
    protected String dbUrl;
    protected String username;
    protected String password;
    protected List<String> column;
    protected String table;
    protected Connection connection;
    protected List<String> preSql;
    protected List<String> postSql;
    protected int batchSize;
    protected Map<String,List<String>> updateKey;
    protected List<String> fullColumn;
    protected TypeConverterInterface typeConverter;

    private static final int DEFAULT_BATCH_SIZE = 1024;

    private final static int SQL_SERVER_MAX_PARAMETER_MARKER = 2000;

    public void setTypeConverterInterface(TypeConverterInterface typeConverter) {
        this.typeConverter = typeConverter;
    }

    public void setDatabaseInterface(DatabaseInterface databaseInterface) {
        this.databaseInterface = databaseInterface;
    }

    public JdbcDataWriter(DataTransferConfig config) {

        super(config);

        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();

        dbUrl = writerConfig.getParameter().getConnection().get(0).getJdbcUrl();

        if(writerConfig.getName().equalsIgnoreCase("mysqlwriter")) {
            String[] splits = dbUrl.split("\\?");

            Map<String,String> paramMap = new HashMap<String,String>();
            if(splits.length > 1) {
                String[] pairs = splits[1].split("&");
                for(String pair : pairs) {
                    String[] leftRight = pair.split("=");
                    paramMap.put(leftRight[0], leftRight[1]);
                }
            }
            paramMap.put("rewriteBatchedStatements", "true");

            StringBuffer sb = new StringBuffer(splits[0]);
            if(paramMap.size() != 0) {
                sb.append("?");
                int index = 0;
                for(Map.Entry<String,String> entry : paramMap.entrySet()) {
                    if(index != 0) {
                        sb.append("&");
                    }
                    sb.append(entry.getKey() + "=" + entry.getValue());
                    index++;
                }
            }

            dbUrl = sb.toString();
        }

        username = writerConfig.getParameter().getStringVal(KEY_USERNAME);
        password = writerConfig.getParameter().getStringVal(KEY_PASSWORD);
        table = writerConfig.getParameter().getConnection().get(0).getTable().get(0);
        preSql = (List<String>) writerConfig.getParameter().getVal(KEY_PRE_SQL);
        postSql = (List<String>) writerConfig.getParameter().getVal(KEY_POST_SQL);
        batchSize = writerConfig.getParameter().getIntVal(KEY_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        column = (List<String>) writerConfig.getParameter().getColumn();
        mode = writerConfig.getParameter().getStringVal(KEY_WRITE_MODE);

        updateKey = (Map<String, List<String>>) writerConfig.getParameter().getVal(KEY_UPDATE_KEY);
        fullColumn = (List<String>) writerConfig.getParameter().getVal(KEY_FULL_COLUMN);
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        JdbcOutputFormatBuilder builder = new JdbcOutputFormatBuilder();
        builder.setDrivername(databaseInterface.getDriverClass());
        builder.setDBUrl(dbUrl);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setBatchInterval(getBatchSize());
        builder.setMonitorUrls(monitorUrls);
        builder.setPreSql(preSql);
        builder.setPostSql(postSql);
        builder.setErrors(errors);
        builder.setErrorRatio(errorRatio);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);
        builder.setDatabaseInterface(databaseInterface);
        builder.setMode(mode);
        builder.setTable(table);
        builder.setColumn(column);
        builder.setFullColumn(fullColumn);
        builder.setUpdateKey(updateKey);
        builder.setTypeConverter(typeConverter);

        OutputFormatSinkFunction sinkFunction = new OutputFormatSinkFunction(builder.finish());
        DataStreamSink<?> dataStreamSink = dataSet.addSink(sinkFunction);
        String sinkName = (databaseInterface.getDatabaseType() + "writer").toLowerCase();
        dataStreamSink.name(sinkName);
        return dataStreamSink;
    }

    /**
     * fix bug:Prepared or callable statement has more than 2000 parameter markers
     */
    private int getBatchSize(){
        if(databaseInterface.getDatabaseType() == EDatabaseType.SQLServer){
            if(column.size() * batchSize >= SQL_SERVER_MAX_PARAMETER_MARKER){
                batchSize = SQL_SERVER_MAX_PARAMETER_MARKER / column.size();
            }
        }

        return batchSize;
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
