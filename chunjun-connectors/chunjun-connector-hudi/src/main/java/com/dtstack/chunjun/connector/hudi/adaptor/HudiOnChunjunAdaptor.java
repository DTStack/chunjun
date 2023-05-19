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
package com.dtstack.chunjun.connector.hudi.adaptor;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;

import org.apache.flink.api.java.typeutils.TypeExtractionException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsInference;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.util.AvroSchemaConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/** hudi adaptor with chunjun */
public class HudiOnChunjunAdaptor {
    private SyncConfig syncConf;
    private Configuration conf;

    private static final Logger LOG = LoggerFactory.getLogger(HudiOnChunjunAdaptor.class);

    public HudiOnChunjunAdaptor(SyncConfig syncConf, Configuration conf) {
        this.syncConf = syncConf;
        this.conf = conf;
    }

    public DataStreamSink createHudiSinkDataStream(
            DataStream<RowData> dataStream, ResolvedSchema schema) throws TypeExtractionException {
        Preconditions.checkNotNull(dataStream);
        RowType rowType = (RowType) schema.toSinkRowDataType().notNull().getLogicalType();
        // setup configuration
        long ckpTimeout =
                dataStream.getExecutionEnvironment().getCheckpointConfig().getCheckpointTimeout();
        conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);
        boolean isNotStreamJob = !conf.getBoolean("isStreamJob", true);
        setupPartition();
        // check hudi configuration
        sanityCheck();
        // init hudi default configuration
        setupConfOptions(rowType);

        // set up default parallelism
        OptionsInference.setupSinkTasks(conf, dataStream.getExecutionConfig().getParallelism());

        // bulk_insert mode
        final String writeOperation = this.conf.get(FlinkOptions.OPERATION);
        if (WriteOperationType.fromValue(writeOperation) == WriteOperationType.BULK_INSERT) {
            return Pipelines.bulkInsert(conf, rowType, dataStream);
        }

        // Append mode
        if (OptionsResolver.isAppendMode(conf)) {
            DataStream<Object> pipeline =
                    Pipelines.append(conf, rowType, dataStream, isNotStreamJob);
            if (OptionsResolver.needsAsyncClustering(conf)) {
                return Pipelines.cluster(conf, rowType, pipeline);
            } else {
                return Pipelines.dummySink(pipeline);
            }
        }

        DataStream<Object> pipeline;
        // bootstrap
        final DataStream<HoodieRecord> hoodieRecordDataStream =
                Pipelines.bootstrap(conf, rowType, dataStream, isNotStreamJob, false);
        // write pipeline
        pipeline = Pipelines.hoodieStreamWrite(conf, hoodieRecordDataStream);
        // compaction
        if (OptionsResolver.needsAsyncCompaction(conf)) {
            // use synchronous compaction for bounded source.
            if (isNotStreamJob) {
                conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
            }
            return Pipelines.compact(conf, pipeline);
        } else {
            return Pipelines.clean(conf, pipeline);
        }
    };

    private void sanityCheck() {
        Map<String, Object> chunjunConfig = this.syncConf.getWriter().getParameter();
        final List<FieldConfig> fieldList = this.syncConf.getWriter().getFieldList();
        Map<String, String> hudiConfig = (Map<String, String>) chunjunConfig.get("hudiConfig");
        String tsField = hudiConfig.get(FlinkOptions.PRECOMBINE_FIELD.key());
        if (StringUtils.isBlank(tsField)) {
            exitAndPrint("No configuration properties: precombine.field.");
        }
        final String tableName = hudiConfig.get(FlinkOptions.TABLE_NAME.key());
        if (StringUtils.isBlank(tableName)) {
            exitAndPrint("No configuration properties: hoodie.table.name.");
        }

        String keys = hudiConfig.get(FlinkOptions.RECORD_KEY_FIELD.key());
        if (StringUtils.isBlank(keys)) {
            exitAndPrint("No configuration properties: hoodie.datasource.write.recordkey.field.");
        }
        boolean checkKeyResult = crucialColumnCheck(keys, fieldList);
        if (!checkKeyResult) {
            exitAndPrint(
                    "The configured field does not appear in the table structure: hoodie.datasource.write.recordkey.");
        }
    }

    /**
     * Check that key columns are present in the table structure.
     *
     * @param keys key columns
     * @param fieldList table structure
     * @return Whether present
     */
    private boolean crucialColumnCheck(String keys, List<FieldConfig> fieldList) {
        String[] key = keys.split(",");
        final Map<String, Boolean> checkMap =
                Arrays.stream(key).collect(Collectors.toMap(String::trim, Boolean::new));
        fieldList.stream().forEach(x -> checkMap.put(x.getName().trim(), true));
        AtomicBoolean checkPass = new AtomicBoolean(true);
        checkMap.forEach((c, r) -> checkPass.set(checkPass.get() && r));
        return checkPass.get();
    }

    private void setupConfOptions(RowType rowType) {
        // Compaction options.
        setupCompactionOptions(this.conf);
        // Hive options.
        setupHiveOptions(conf);
        // Infer avro schema from physical DDL schema.
        inferAvroSchema(conf, rowType);
        // write options
        setupWriteOptions(conf);
    }

    private static void setupWriteOptions(Configuration conf) {
        if (FlinkOptions.isDefaultValueDefined(conf, FlinkOptions.OPERATION)
                && OptionsResolver.isCowTable(conf)) {
            conf.setBoolean(FlinkOptions.PRE_COMBINE, true);
        }
    }

    /** Sets up the compaction options from the table definition. */
    private void setupCompactionOptions(Configuration conf) {
        int commitsToRetain = conf.getInteger(FlinkOptions.CLEAN_RETAIN_COMMITS);
        int minCommitsToKeep = conf.getInteger(FlinkOptions.ARCHIVE_MIN_COMMITS);
        if (commitsToRetain >= minCommitsToKeep) {
            LOG.info(
                    "Table option [{}] is reset to {} to be greater than {}={},\n"
                            + "to avoid risk of missing data from few instants in incremental pull",
                    FlinkOptions.ARCHIVE_MIN_COMMITS.key(),
                    commitsToRetain + 10,
                    FlinkOptions.CLEAN_RETAIN_COMMITS.key(),
                    commitsToRetain);
            conf.setInteger(FlinkOptions.ARCHIVE_MIN_COMMITS, commitsToRetain + 10);
            conf.setInteger(FlinkOptions.ARCHIVE_MAX_COMMITS, commitsToRetain + 20);
        }
    }

    /** Sets up the hive options from the table definition. */
    private static void setupHiveOptions(Configuration conf) {
        if (!conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING)
                && FlinkOptions.isDefaultValueDefined(
                        conf, FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME)) {
            conf.setString(
                    FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME,
                    MultiPartKeysValueExtractor.class.getName());
        }
    }

    /**
     * Inferences the deserialization Avro schema from the table schema (e.g. the DDL) if both
     * options {@link FlinkOptions#SOURCE_AVRO_SCHEMA_PATH} and {@link
     * FlinkOptions#SOURCE_AVRO_SCHEMA} are not specified.
     *
     * @param conf The configuration.
     * @param rowType The specified table row type.
     */
    private void inferAvroSchema(Configuration conf, LogicalType rowType) {
        if (!conf.getOptional(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH).isPresent()
                && !conf.getOptional(FlinkOptions.SOURCE_AVRO_SCHEMA).isPresent()) {
            String inferredSchema = AvroSchemaConverter.convertToSchema(rowType).toString();
            conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, inferredSchema);
        }
    }

    /** * setup partition from comfiguration */
    private void setupPartition() {
        // Tweak the key gen class if possible.
        conf.setString(FlinkOptions.PARTITION_DEFAULT_NAME, "__HIVE_DEFAULT_PARTITION__");
        final String[] partitions = conf.getString(FlinkOptions.PARTITION_PATH_FIELD).split(",");
        if (partitions.length == 1 && partitions[0].equals("")) {
            conf.setString(
                    FlinkOptions.KEYGEN_CLASS_NAME, NonpartitionedAvroKeyGenerator.class.getName());
            return;
        }
        final String[] pks = conf.getString(FlinkOptions.RECORD_KEY_FIELD).split(",");
        boolean complexHoodieKey = pks.length > 1 || partitions.length > 1;
        if (complexHoodieKey
                && FlinkOptions.isDefaultValueDefined(conf, FlinkOptions.KEYGEN_CLASS_NAME)) {
            conf.setString(FlinkOptions.KEYGEN_CLASS_NAME, ComplexAvroKeyGenerator.class.getName());
        }
    }

    private void exitAndPrint(String message) {
        LOG.error(message);
        System.exit(-1);
    }
}
