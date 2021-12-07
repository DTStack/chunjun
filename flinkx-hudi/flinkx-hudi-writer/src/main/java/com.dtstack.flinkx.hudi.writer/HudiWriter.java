/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.hudi.writer;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.hudi.HudiConfigKeys;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;
import org.apache.hudi.common.model.HoodieTableType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.hudi.HudiConfigKeys.*;

/**
 * @author fengjiangtao_yewu@cmss.chinamobile.com
 * @date 2021-08-10
 */

public class HudiWriter extends BaseDataWriter {
    /**
     * org.apache.hudi.common.model.WriteOperationType
     * UPSERT | UPSERT_PREPPED
     */
    protected String writeOperation;

    /**
     * Table name to register to Hive metastore
     */
    protected String tableName;

    /**
     * Type of table to write. COPY_ON_WRITE (or) MERGE_ON_READ
     */
    protected String tableType;

    /**
     * Base path for the target hoodie table.
     */
    protected String path;

    /**
     * Async Compaction, enabled by default for MOR
     */
    protected boolean compress;

    protected List<MetaColumn> metaColumns;
    protected String defaultFS;
    protected String recordKey;
    protected String hiveJdbcUrl;
    protected String hiveMetastore;
    protected String hiveUser;
    protected String hivePass;
    protected int batchInterval;
    protected List<String> partitionFields;
    protected Map<String, Object> hadoopConfig;

    public HudiWriter(DataTransferConfig config) {
        super(config);

        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        writeOperation = writerConfig.getParameter().getStringVal(HudiConfigKeys.KEY_WRITE_MODE);
        tableName = writerConfig.getParameter().getStringVal(HudiConfigKeys.KEY_TABLE_NAME);
        tableType = writerConfig.getParameter().getStringVal(HudiConfigKeys.KEY_TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());
        recordKey = writerConfig.getParameter().getStringVal(HudiConfigKeys.KEY_TABLE_RECORD_KEY, "id");
        path = writerConfig.getParameter().getStringVal(HudiConfigKeys.KEY_PATH);
        compress = writerConfig.getParameter().getBooleanVal(HudiConfigKeys.KEY_COMPRESS, Boolean.FALSE);
        metaColumns = MetaColumn.getMetaColumns(writerConfig.getParameter().getColumn());
        defaultFS = writerConfig.getParameter().getStringVal(HudiConfigKeys.KEY_DEFAULT_FS);
        hiveJdbcUrl = writerConfig.getParameter().getStringVal(HudiConfigKeys.KEY_HIVE_JDBC);
        hiveMetastore = writerConfig.getParameter().getStringVal(HudiConfigKeys.KEY_HIVE_METASTORE);
        hiveUser = writerConfig.getParameter().getStringVal(HudiConfigKeys.KEY_HIVE_USER, "");
        hivePass = writerConfig.getParameter().getStringVal(HudiConfigKeys.KEY_HIVE_PASS, "");
        batchInterval = writerConfig.getParameter().getIntVal(HudiConfigKeys.KEY_BATCH_INTERVAL, 1);
        String partitionField = writerConfig.getParameter().getStringVal(HudiConfigKeys.KEY_PARTITION_FIELDS);
        partitionFields = StringUtils.isNotBlank(partitionField) ? Arrays.asList(StringUtils.split(partitionField, ",")) : new ArrayList<>();
        hadoopConfig = (Map<String, Object>) writerConfig.getParameter().getVal(HudiConfigKeys.KEY_HADOOP_CONFIG);
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        HudiOutputformatBuilder builder = new HudiOutputformatBuilder();
        builder.setTableName(tableName);
        builder.setTableType(tableType);
        builder.setRecordKey(recordKey);
        builder.setPath(path);
        builder.setHadoopConf(hadoopConfig);
        builder.setDefaultFS(defaultFS);
        builder.setHiveJdbcUrl(hiveJdbcUrl);
        builder.setHiveMetastore(hiveMetastore);
        builder.setHiveUser(hiveUser);
        builder.setHivePass(hivePass);
        builder.setSchema(buildSchema(metaColumns, tableName));
        builder.setColumns(metaColumns);
        builder.setPartitionFields(partitionFields);
        builder.setBatchInterval(batchInterval);

        return createOutput(dataSet, builder.finish());
    }

    /**
     * Transform MetaColumn to org.apache.avro.Schema.
     *
     * @param metaColumns
     * @param tableName
     * @return
     */
    private String buildSchema(List<MetaColumn> metaColumns, String tableName) {
        String[] dbTableName = StringUtils.split(tableName, ".");
        JSONArray jsonArray = new JSONArray();
        metaColumns.forEach(metaColumn -> {
            JSONObject jsonField = new JSONObject();
            jsonField.put(KEY_COLUMN_NAME, metaColumn.getName());
            jsonField.put(KEY_COLUMN_TYPE, metaColumn.getType());
            jsonArray.add(jsonField);
        });

        JSONObject schemaJson = new JSONObject();
        schemaJson.put(KEY_COLUMN_NAME, dbTableName[1]);
        schemaJson.put(KEY_COLUMN_TYPE, KEY_TABLE_TYPE_RECORD);
        schemaJson.put(KEY_SCHEMA_FIELDS, jsonArray);

        return schemaJson.toString();
    }

}
