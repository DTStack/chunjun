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

import com.dtstack.flinkx.hudi.HudiConfigKeys;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.FileSystemUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.StringUtil;
import com.google.common.collect.Lists;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.hive.ddl.HiveSyncMode;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.org.apache.avro.Schema;
import org.apache.hudi.org.apache.avro.generic.GenericData;
import org.apache.hudi.org.apache.avro.generic.GenericRecord;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.dtstack.flinkx.hudi.HudiConfigKeys.KEY_HADOOP_USER_NAME;

/**
 * Reference: org.apache.hudi.examples.java.HoodieJavaWriteClientExample
 *
 * @author fengjiangtao_yewu@cmss.chinamobile.com
 * @date 2021-08-10
 */

public class HudiOutputFormat extends BaseRichOutputFormat {
    protected String tableName;
    protected String tableType;
    protected String path;
    protected String schema;
    protected String defaultFS;
    protected String recordKey;
    protected String hiveJdbcUrl;
    protected String hiveMetastore;
    protected String hiveUser;
    protected String hivePass;
    protected Schema avroSchema;
    protected String[] dbTableName;
    protected Map<String, Object> hadoopConfig;
    protected List<MetaColumn> metaColumns;
    protected List<String> partitionFields;

    protected transient HoodieJavaWriteClient<HoodieAvroPayload> client;
    protected FileSystem fs;
    protected Configuration hadoopConfiguration;
    protected HiveConf hiveConf;
    protected HiveSyncConfig hiveSyncConfig;

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        dbTableName = org.apache.commons.lang3.StringUtils.split(tableName, ".");
        // Create the write client to write some records in
        HoodieWriteConfig hudiWriteConfig = HoodieWriteConfig.newBuilder()
                .withEngineType(EngineType.FLINK).withPath(path)
                .withSchema(schema).withParallelism(numTasks, numTasks)
                .withDeleteParallelism(numTasks).forTable(dbTableName[1])
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();

        hadoopConfiguration = FileSystemUtil.getConfiguration(hadoopConfig, defaultFS);
        client = new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConfiguration), hudiWriteConfig);
        avroSchema = new Schema.Parser().parse(schema);

        if (hadoopConfig.containsKey(KEY_HADOOP_USER_NAME)) {
            // Config the HADOOP_USER_NAME for permission.
            LOG.info("Default System HADOOP_USER_NAME:" + System.getProperty(KEY_HADOOP_USER_NAME));
            System.setProperty(KEY_HADOOP_USER_NAME, hadoopConfig.get(KEY_HADOOP_USER_NAME).toString());
            LOG.info("Change System HADOOP_USER_NAME:" + System.getProperty(KEY_HADOOP_USER_NAME));
        }

        LOG.info("Init hudi table schema:[{}]", schema);
        initTable(hadoopConfiguration, dbTableName[1]);
        initHiveConf();
    }

    @Override
    protected void writeSingleRecordInternal(Row row) {
        String newCommitTime = client.startCommit();
        HoodieRecord<HoodieAvroPayload> hoodieRecord = buildHudiRecords(row);
        List<HoodieRecord<HoodieAvroPayload>> records = Lists.newArrayList(hoodieRecord).stream()
                .filter(record -> record != null).collect(Collectors.toList());

        if (records.size() > 0) {
            client.upsert(records, newCommitTime);
            syncHiveMeta();
        }
    }

    /**
     * Multiple write cost less when every writing needs to sync hive Metastore.
     */
    @Override
    protected void writeMultipleRecordsInternal() {
        String newCommitTime = client.startCommit();

        List<HoodieRecord<HoodieAvroPayload>> records = rows.stream().filter(record -> record != null)
                .map(this::buildHudiRecords).collect(Collectors.toList());
        if (records.size() > 0) {
            client.upsert(records, newCommitTime);
            syncHiveMeta();
        }
    }

    /**
     * Build hudi record by row data.
     *
     * @param row
     * @return
     */
    private HoodieRecord<HoodieAvroPayload> buildHudiRecords(Row row) {
        HoodieKey key = new HoodieKey();
        // Set first key as recordKey
        Option<GenericRecord> genericRecord;
        HoodieAvroPayload hoodieAvroPayload;
        HoodieRecord<HoodieAvroPayload> hoodieRecord;
        try {
            key.setRecordKey(recordKey);
            // TODO Next version will support partition table.
            key.setPartitionPath("");

            genericRecord = Option.of(buildGenericRecord(row));
            if (!genericRecord.isPresent()) {
                return null;
            }
            hoodieAvroPayload = new HoodieAvroPayload(genericRecord);
            hoodieRecord = new HoodieRecord<>(key, hoodieAvroPayload);
        } catch (Exception e) {
            LOG.error("Build hudi records err. Row:" + row.toString(), e);
            throw new RuntimeException(e);
        }

        return hoodieRecord;
    }

    /**
     * Init the table
     *
     * @param hadoopConf
     * @param splitTableName
     */
    private void initTable(Configuration hadoopConf, String splitTableName) {
        // initialize the table, if not done already
        Path path = new Path(this.path);
        try {
            fs = FSUtils.getFs(this.path, hadoopConf);
            if (!fs.exists(path)) {
                HoodieTableMetaClient metaClient = HoodieTableMetaClient.withPropertyBuilder()
                        .setTableType(tableType)
                        .setTableName(splitTableName)
                        .setPayloadClassName(HoodieAvroPayload.class.getName())
                        .initTable(hadoopConf, this.path);

                LOG.info("Hudi table init done.", metaClient.getMetaPath());
            }
        } catch (Exception e) {
            LOG.warn("Hudi table init err. " + tableName, e);
            throw new RuntimeException("Create hudi table failed:" + splitTableName + ", " + e.getMessage());
        }
    }

    /**
     * Init hive configuration.
     */
    private void initHiveConf() {
        hiveConf = new HiveConf();
        hiveConf.set(HudiConfigKeys.KEY_HIVE_METASTORE_URIS, hiveMetastore);
        hiveConf.set(HudiConfigKeys.KEY_HA_DEFAULT_FS, defaultFS);

        Iterator<Map.Entry<String, String>> iterator = hadoopConfiguration.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            hiveConf.set(entry.getKey(), entry.getValue());
        }

        hiveSyncConfig = new HiveSyncConfig();
        hiveSyncConfig.jdbcUrl = this.hiveJdbcUrl;
        hiveSyncConfig.autoCreateDatabase = true;
        hiveSyncConfig.databaseName = this.dbTableName[0];
        hiveSyncConfig.tableName = this.dbTableName[1];
        hiveSyncConfig.basePath = this.path;
        hiveSyncConfig.assumeDatePartitioning = true;
        hiveSyncConfig.usePreApacheInputFormat = false;
        hiveSyncConfig.createManagedTable = true;
        if (org.apache.commons.lang3.StringUtils.isNotBlank(this.hiveUser)) {
            hiveSyncConfig.hiveUser = this.hiveUser;
        }
        if (org.apache.commons.lang3.StringUtils.isNotBlank(this.hivePass)) {
            hiveSyncConfig.hivePass = this.hivePass;
        }
        if (partitionFields.size() > 0) {
            hiveSyncConfig.partitionFields = partitionFields;
        }

        // Default use HIVEQL sync mode
        hiveSyncConfig.syncMode = HiveSyncMode.HIVEQL.name();
    }

    /**
     * Sync the hive meta after every upsert batch.
     *
     * @return
     */
    private boolean syncHiveMeta() {
        try {
            HiveSyncTool hiveSyncTool = new HiveSyncTool(hiveSyncConfig, hiveConf, fs);
            hiveSyncTool.syncHoodieTable();
        } catch (Exception e) {
            LOG.error("Create hive meta failed " + tableName, e);
            throw new RuntimeException("Sync hudi to hive metastore failed.", e);
        }

        return true;
    }

    /**
     * Schema for example:
     * {
     * "type": "record",
     * "name": "triprec",
     * "fields": [
     * {"name": "ts","type": "long"},
     * {"name": "uuid","type": "string"},
     * {"name": "begin_lat","type": "double"}
     * ]
     * }
     *
     * @param row
     * @return
     */
    private GenericRecord buildGenericRecord(Row row) {
        GenericRecord rec = new GenericData.Record(avroSchema);

        try {
            int arity = row.getArity();
            if (metaColumns != null && metaColumns.size() > 0) {
                for (int i = 0; i < arity; i++) {
                    String value = StringUtils.arrayAwareToString(row.getField(i));
                    rec.put(metaColumns.get(i).getName(), StringUtil.string2col(value, metaColumns.get(i).getType(), null));
                }
            } else {
                Map<String, Object> map = GsonUtil.GSON.fromJson(row.getField(0).toString(), GsonUtil.gsonMapTypeToken);
                map.keySet().stream().forEach(key -> rec.put(key, map.get(key)));
            }
        } catch (Exception e) {
            LOG.warn("Build genericRecord err." + row.toString(), e);
            return null;
        }

        return rec;
    }

    /**
     * Close client.
     */
    @Override
    public void closeInternal() {
        LOG.warn("Hudi output closeInternal.");
        if (null != client) {
            client.close();
        }
    }
}
