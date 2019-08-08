/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.hive.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.hive.TableInfo;
import com.dtstack.flinkx.hive.util.HiveUtil;
import com.dtstack.flinkx.writer.DataWriter;
import com.google.gson.Gson;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.DtOutputFormatSinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.hive.HdfsConfigKeys.*;

/**
 * The writer plugin of Hdfs
 * <p>
 * Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class HiveWriter extends DataWriter {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

//    protected String defaultFS;

//    private String fileType;
    private String store;

    private String partition;

    //    protected String path;

    private String delimiter;

    private String compress;

    private int interval;

    private int bufferSize;

//    protected String fileName;

    private Map<String, TableInfo> tableInfos;

    private Map<String, String> distributeTableMapping;

//    protected List<String> columnName;

//    protected List<String> columnType;

    private Map<String, String> hadoopConfig;

    private String charSet;

//    protected List<String> fullColumnName;

//    protected List<String> fullColumnType;

//    protected static final String DATA_SUBDIR = ".data";
//
//    protected static final String FINISHED_SUBDIR = ".finished";
//
//    protected static final String SP = "/";

//    protected int rowGroupSize;

    private long maxFileSize;

    private String jdbcUrl;

    private String database;

    private String username;

    private String password;

    private String tableBasePath;

    private boolean autoCreateTable;

    private Gson gson = new Gson();

    public HiveWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        hadoopConfig = (Map<String, String>) writerConfig.getParameter().getVal(KEY_HADOOP_CONFIG_MAP);
        store = writerConfig.getParameter().getStringVal(KEY_STORE);
        partition = writerConfig.getParameter().getStringVal(KEY_PARTITION, "pt");
//        defaultFS = writerConfig.getParameter().getStringVal(KEY_DEFAULT_FS);
//        path = writerConfig.getParameter().getStringVal(KEY_PATH);
        delimiter = writerConfig.getParameter().getStringVal(KEY_DELIMITER, "\u0001");
        charSet = writerConfig.getParameter().getStringVal(KEY_ENCODING);
//        rowGroupSize = writerConfig.getParameter().getIntVal(KEY_ROW_GROUP_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE);
        maxFileSize = writerConfig.getParameter().getLongVal(KEY_MAX_FILE_SIZE, 1024 * 1024 * 1024);

        compress = writerConfig.getParameter().getStringVal(KEY_COMPRESS);
        interval = writerConfig.getParameter().getIntVal(KEY_INTERVAL, 60 * 60 * 1000);
        bufferSize = writerConfig.getParameter().getIntVal(KEY_BUFFER_SIZE, 128 * 1024 * 1024);

//        fullColumnName = (List<String>) writerConfig.getParameter().getVal(KEY_FULL_COLUMN_NAME_LIST);
//        fullColumnType = (List<String>) writerConfig.getParameter().getVal(KEY_FULL_COLUMN_TYPE_LIST);

        mode = writerConfig.getParameter().getStringVal(KEY_WRITE_MODE);
        jdbcUrl = writerConfig.getParameter().getStringVal(KEY_JDBC_URL);
        formatHiveJdbcUrlInfo();
        password = writerConfig.getParameter().getStringVal(KEY_USERNAME);
        username = writerConfig.getParameter().getStringVal(KEY_PASSWORD);

        String distributeTable = writerConfig.getParameter().getStringVal(KEY_DISTRIBUTE_TABLE);
        formatHiveDistributeInfo(distributeTable);

        String tablesColumn = writerConfig.getParameter().getStringVal(KEY_TABLE_COLUMN);
        formatHiveTableInfo(tablesColumn);

        String analyticalRules = writerConfig.getParameter().getStringVal(KEY_ANALYTICAL_RULES);
        if (StringUtils.isBlank(analyticalRules)) {
            tableBasePath = tableInfos.entrySet().iterator().next().getValue().getTableName();
        } else {
            tableBasePath = analyticalRules;
            autoCreateTable = true;
        }
    }

    private void formatHiveDistributeInfo(String distributeTable) {
        /**
         * 分表的映射关系
         * distributeTableMapping 的数据结构为<tableName,groupName>
         * tableInfos的数据结构为<groupName,TableInfo>
         */
        if (StringUtils.isNotBlank(distributeTable)) {
            distributeTableMapping = new HashMap<String, String>();
            Map<String, Object> distributeTableMap = gson.fromJson(distributeTable, Map.class);
            for (Map.Entry<String, Object> entry : distributeTableMap.entrySet()) {
                String groupName = entry.getKey();
                List<String> groupTables = (List<String>) entry.getValue();
                for (String tableName : groupTables) {
                    distributeTableMapping.put(tableName, groupName);
                }
            }
        }
    }

    private void formatHiveJdbcUrlInfo() {
        if (jdbcUrl.contains(";principal=")) {
            String[] jdbcStr = jdbcUrl.split(";principal=");
            jdbcUrl = jdbcStr[0];
        }
        int anythingIdx = StringUtils.indexOf(jdbcUrl, '?');
        if (anythingIdx != -1) {
            database = StringUtils.substring(jdbcUrl, StringUtils.lastIndexOf(jdbcUrl, '/') + 1, anythingIdx);
        } else {
            database = StringUtils.substring(jdbcUrl, StringUtils.lastIndexOf(jdbcUrl, '/') + 1);
        }
    }

    private void formatHiveTableInfo(String tablesColumn) {
        if (StringUtil.isNotEmpty(tablesColumn)) {
            tableInfos = new HashMap<String, TableInfo>();
            Map<String, Object> tableColumnMap = gson.fromJson(tablesColumn, Map.class);
            for (Map.Entry<String, Object> entry : tableColumnMap.entrySet()) {
                String tableName = entry.getKey();
                List<Map<String, Object>> tableColumns = (List<Map<String, Object>>) entry.getValue();
                TableInfo tableInfo = new TableInfo(tableColumns.size());
                tableInfo.setDatabase(database);
                tableInfo.addPartition(partition);
                tableInfo.setDelimiter(delimiter);
                tableInfo.setStore(store);
                tableInfo.setTableName(tableName);
                for (Map<String, Object> column : tableColumns) {
                    tableInfo.addColumnAndType(MapUtils.getString(column, HiveUtil.TABLE_COLUMN_KEY), HiveUtil.convertType(MapUtils.getString(column, HiveUtil.TABLE_COLUMN_TYPE)));
                }
                String createTableSql = HiveUtil.getCreateTableHql(tableInfo);
                tableInfo.setCreateTableSql(createTableSql);

                tableInfos.put(tableName, tableInfo);
            }
        }
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        HdfsOutputFormatBuilder builder = new HdfsOutputFormatBuilder(store);
        builder.setHadoopConfig(hadoopConfig);
//        builder.setDefaultFS(defaultFS);
//        builder.setPath(path);
//        builder.setFileName(fileName);
        builder.setWriteMode(mode);
//        builder.setColumnNames(columnName);
//        builder.setColumnTypes(columnType);
        builder.setCompress(compress);
        builder.setMonitorUrls(monitorUrls);
        builder.setErrors(errors);
        builder.setErrorRatio(errorRatio);
//        builder.setFullColumnNames(fullColumnName);
//        builder.setFullColumnTypes(fullColumnType);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);
        builder.setCharSetName(charSet);
        builder.setDelimiter(delimiter);
//        builder.setRowGroupSize(rowGroupSize);
        builder.setRestoreConfig(restoreConfig);
        builder.setMaxFileSize(maxFileSize);

        DtOutputFormatSinkFunction sinkFunction = new DtOutputFormatSinkFunction(builder.finish());
        DataStreamSink<?> dataStreamSink = dataSet.addSink(sinkFunction);

        dataStreamSink.name("hivewriter");

        return dataStreamSink;
    }
}
