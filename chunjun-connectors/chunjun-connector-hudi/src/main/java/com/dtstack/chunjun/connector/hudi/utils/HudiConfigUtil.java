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

package com.dtstack.chunjun.connector.hudi.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTableFactory;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class HudiConfigUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HoodieTableFactory.class);

    public static void setupTableOptions(
            String basePath, Configuration conf, ResolvedSchema schema) {
        conf.setString(FlinkOptions.RECORD_KEY_FIELD, "uuid");
        if (schema.getPrimaryKey().isPresent()) {
            List<String> columns = schema.getPrimaryKey().get().getColumns();
            String keys = String.join(",", columns);
            conf.setString(FlinkOptions.RECORD_KEY_FIELD, keys);
        }
        StreamerUtil.getTableConfig(basePath, HadoopConfigurations.getHadoopConf(conf))
                .ifPresent(
                        tableConfig -> {
                            if (tableConfig.contains(HoodieTableConfig.RECORDKEY_FIELDS)
                                    && !conf.contains(FlinkOptions.RECORD_KEY_FIELD)) {
                                conf.setString(
                                        FlinkOptions.RECORD_KEY_FIELD,
                                        tableConfig.getString(HoodieTableConfig.RECORDKEY_FIELDS));
                            }
                            if (tableConfig.contains(HoodieTableConfig.PRECOMBINE_FIELD)
                                    && !conf.contains(FlinkOptions.PRECOMBINE_FIELD)) {
                                conf.setString(
                                        FlinkOptions.PRECOMBINE_FIELD,
                                        tableConfig.getString(HoodieTableConfig.PRECOMBINE_FIELD));
                            }
                            if (tableConfig.contains(
                                            HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE)
                                    && !conf.contains(FlinkOptions.HIVE_STYLE_PARTITIONING)) {
                                conf.setBoolean(
                                        FlinkOptions.HIVE_STYLE_PARTITIONING,
                                        tableConfig.getBoolean(
                                                HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE));
                            }
                        });
    }

    public static void setupConfOptions(
            Configuration conf,
            String tableName,
            ResolvedSchema schema,
            ObjectIdentifier tablePath,
            String keys,
            String partitions) {
        setupConfOptions(conf, tableName, schema, keys, partitions);
        setupHiveOptions(conf, tablePath);
    }

    public static void setupConfOptions(
            Configuration conf,
            String tableName,
            ResolvedSchema schema,
            String keys,
            String partitions) {
        // table name
        conf.setString(FlinkOptions.TABLE_NAME.key(), tableName);
        // hoodie key about options
        setupHoodieKeyOptions(conf, keys, partitions);
        // compaction options
        setupCompactionOptions(conf);
        // read options
        setupReadOptions(conf);
        // write options
        setupWriteOptions(conf);
        // infer avro schema from physical DDL schema
        inferAvroSchema(conf, schema.toPhysicalRowDataType().notNull().getLogicalType());
    }

    /**
     * Sets up the hoodie key options (e.g. record key and partition key) from the table definition.
     */
    private static void setupHoodieKeyOptions(Configuration conf, String keys, String partitions) {
        List<String> pkColumns = Arrays.asList(keys.split(","));
        if (pkColumns.size() > 0) {
            // the PRIMARY KEY syntax always has higher priority than option
            // FlinkOptions#RECORD_KEY_FIELD
            String recordKey = String.join(",", pkColumns);
            conf.setString(FlinkOptions.RECORD_KEY_FIELD, recordKey);
        }
        List<String> partitionKeys = Arrays.asList(partitions.split(","));
        if (partitionKeys.size() > 0) {
            // the PARTITIONED BY syntax always has higher priority than option
            // FlinkOptions#PARTITION_PATH_FIELD
            conf.setString(FlinkOptions.PARTITION_PATH_FIELD, String.join(",", partitionKeys));
        }
        // set index key for bucket index if not defined
        if (conf.getString(FlinkOptions.INDEX_TYPE).equals(HoodieIndex.IndexType.BUCKET.name())) {
            if (conf.getString(FlinkOptions.INDEX_KEY_FIELD).isEmpty()) {
                conf.setString(
                        FlinkOptions.INDEX_KEY_FIELD,
                        conf.getString(FlinkOptions.RECORD_KEY_FIELD));
            } else {
                Set<String> recordKeySet =
                        Arrays.stream(conf.getString(FlinkOptions.RECORD_KEY_FIELD).split(","))
                                .collect(Collectors.toSet());
                Set<String> indexKeySet =
                        Arrays.stream(conf.getString(FlinkOptions.INDEX_KEY_FIELD).split(","))
                                .collect(Collectors.toSet());
                if (!recordKeySet.containsAll(indexKeySet)) {
                    throw new HoodieValidationException(
                            FlinkOptions.INDEX_KEY_FIELD
                                    + " should be a subset of or equal to the recordKey fields");
                }
            }
        }
    }

    /** Sets up the compaction options from the table definition. */
    private static void setupCompactionOptions(Configuration conf) {
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
    private static void setupHiveOptions(Configuration conf, ObjectIdentifier tablePath) {
        if (!conf.contains(FlinkOptions.HIVE_SYNC_DB)) {
            conf.setString(FlinkOptions.HIVE_SYNC_DB, tablePath.getDatabaseName());
        }
        if (!conf.contains(FlinkOptions.HIVE_SYNC_TABLE)) {
            conf.setString(FlinkOptions.HIVE_SYNC_TABLE, tablePath.getObjectName());
        }
    }

    /**
     * Inferences the deserialization Avro schema from the table schema (e.g. the DDL) if both
     * options {@link FlinkOptions#SOURCE_AVRO_SCHEMA_PATH} and {@link
     * FlinkOptions#SOURCE_AVRO_SCHEMA} are not specified.
     *
     * @param conf The configuration
     * @param rowType The specified table row type
     */
    private static void inferAvroSchema(Configuration conf, LogicalType rowType) {
        if (!conf.getOptional(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH).isPresent()
                && !conf.getOptional(FlinkOptions.SOURCE_AVRO_SCHEMA).isPresent()) {
            String inferredSchema = AvroSchemaConverter.convertToSchema(rowType).toString();
            conf.setString(FlinkOptions.SOURCE_AVRO_SCHEMA, inferredSchema);
        }
    }

    /** Sets up the read options from the table definition. */
    private static void setupReadOptions(Configuration conf) {
        if (OptionsResolver.isIncrementalQuery(conf)) {
            conf.setString(FlinkOptions.QUERY_TYPE, FlinkOptions.QUERY_TYPE_INCREMENTAL);
        }
    }

    /** Sets up the write options from the table definition. */
    private static void setupWriteOptions(Configuration conf) {
        if (FlinkOptions.isDefaultValueDefined(conf, FlinkOptions.OPERATION)
                && OptionsResolver.isCowTable(conf)) {
            conf.setBoolean(FlinkOptions.PRE_COMBINE, true);
        }
    }

    /**
     * The sanity check.
     *
     * @param conf The table options
     * @param schema The table schema
     */
    public static void sanityCheck(Configuration conf, ResolvedSchema schema) {
        List<String> fields = schema.getColumnNames();

        // validate record key in pk absence.
        if (!schema.getPrimaryKey().isPresent()) {
            Arrays.stream(conf.get(FlinkOptions.RECORD_KEY_FIELD).split(","))
                    .filter(field -> !fields.contains(field))
                    .findAny()
                    .ifPresent(
                            f -> {
                                throw new ValidationException(
                                        "Field '"
                                                + f
                                                + "' does not exist in the table schema."
                                                + "Please define primary key or modify 'hoodie.datasource.write.recordkey.field' option.");
                            });
        }

        // validate pre_combine key
        String preCombineField = conf.get(FlinkOptions.PRECOMBINE_FIELD);
        if (!fields.contains(preCombineField)) {
            throw new ValidationException(
                    "Field "
                            + preCombineField
                            + " does not exist in the table schema."
                            + "Please check 'write.precombine.field' option.");
        }
    }
}
