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
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.dtstack.flinkx.rdb.datawriter.JdbcConfigKeys.KEY_BATCH_SIZE;
import static com.dtstack.flinkx.rdb.datawriter.JdbcConfigKeys.KEY_FULL_COLUMN;
import static com.dtstack.flinkx.rdb.datawriter.JdbcConfigKeys.KEY_INSERT_SQL_MODE;
import static com.dtstack.flinkx.rdb.datawriter.JdbcConfigKeys.KEY_PASSWORD;
import static com.dtstack.flinkx.rdb.datawriter.JdbcConfigKeys.KEY_POST_SQL;
import static com.dtstack.flinkx.rdb.datawriter.JdbcConfigKeys.KEY_PRE_SQL;
import static com.dtstack.flinkx.rdb.datawriter.JdbcConfigKeys.KEY_UPDATE_KEY;
import static com.dtstack.flinkx.rdb.datawriter.JdbcConfigKeys.KEY_USERNAME;
import static com.dtstack.flinkx.rdb.datawriter.JdbcConfigKeys.KEY_WRITE_MODE;

/**
 * The Writer plugin for any database that can be connected via JDBC.
 * <p>
 * Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class JdbcDataWriter extends BaseDataWriter {

    protected DatabaseInterface databaseInterface;
    protected String dbUrl;
    protected String username;
    protected String password;
    protected List<String> column;
    protected String table;
    protected List<String> preSql;
    protected List<String> postSql;
    protected int batchSize;
    protected Map<String, List<String>> updateKey;
    protected List<String> fullColumn;
    protected TypeConverterInterface typeConverter;
    protected Properties properties;

    /**
     * just for postgresql,use copy replace insert
     */
    protected String insertSqlMode;

    private static final int DEFAULT_BATCH_SIZE = 1024;

    public void setTypeConverterInterface(TypeConverterInterface typeConverter) {
        this.typeConverter = typeConverter;
    }

    public void setDatabaseInterface(DatabaseInterface databaseInterface) {
        this.databaseInterface = databaseInterface;
    }

    @SuppressWarnings("unchecked")
    public JdbcDataWriter(DataTransferConfig config) {

        super(config);

        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();

        dbUrl = writerConfig.getParameter().getConnection().get(0).getJdbcUrl();
        username = writerConfig.getParameter().getStringVal(KEY_USERNAME);
        password = writerConfig.getParameter().getStringVal(KEY_PASSWORD);
        table = writerConfig.getParameter().getConnection().get(0).getTable().get(0);
        preSql = (List<String>) writerConfig.getParameter().getVal(KEY_PRE_SQL);
        postSql = (List<String>) writerConfig.getParameter().getVal(KEY_POST_SQL);
        batchSize = writerConfig.getParameter().getIntVal(KEY_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        column = MetaColumn.getColumnNames(writerConfig.getParameter().getColumn());
        mode = writerConfig.getParameter().getStringVal(KEY_WRITE_MODE);

        updateKey = (Map<String, List<String>>) writerConfig.getParameter().getVal(KEY_UPDATE_KEY);
        fullColumn = (List<String>) writerConfig.getParameter().getVal(KEY_FULL_COLUMN);

        insertSqlMode = writerConfig.getParameter().getStringVal(KEY_INSERT_SQL_MODE);
        properties = writerConfig.getParameter().getProperties(JdbcConfigKeys.KEY_PROPERTIES, null);
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        JdbcOutputFormatBuilder builder = getBuilder();
        builder.setDriverName(databaseInterface.getDriverClass());
        builder.setDbUrl(dbUrl);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setBatchInterval(batchSize);
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
        builder.setRestoreConfig(restoreConfig);
        builder.setInsertSqlMode(insertSqlMode);
        builder.setProperties(properties);

        return createOutput(dataSet, builder.finish());
    }

    protected JdbcOutputFormatBuilder getBuilder() {
        throw new RuntimeException("子类必须覆盖getBuilder方法");
    }
}
