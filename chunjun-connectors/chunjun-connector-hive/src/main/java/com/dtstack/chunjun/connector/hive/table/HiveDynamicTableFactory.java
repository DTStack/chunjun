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
package com.dtstack.chunjun.connector.hive.table;

import com.dtstack.chunjun.connector.hdfs.options.HdfsOptions;
import com.dtstack.chunjun.connector.hive.conf.HiveConfig;
import com.dtstack.chunjun.connector.hive.options.HiveOptions;
import com.dtstack.chunjun.connector.hive.sink.HiveDynamicTableSink;
import com.dtstack.chunjun.table.options.BaseFileOptions;
import com.dtstack.chunjun.table.options.SinkOptions;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Date: 2021/06/22 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HiveDynamicTableFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "hive-x";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HdfsOptions.DEFAULT_FS);
        options.add(HdfsOptions.FILE_TYPE);

        options.add(HiveOptions.JDBC_URL);
        options.add(HiveOptions.TABLE_NAME);

        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>(32);
        options.add(SinkOptions.SINK_PARALLELISM);

        options.add(BaseFileOptions.FILE_NAME);
        options.add(BaseFileOptions.WRITE_MODE);
        options.add(BaseFileOptions.COMPRESS);
        options.add(BaseFileOptions.ENCODING);
        options.add(BaseFileOptions.MAX_FILE_SIZE);
        options.add(BaseFileOptions.NEXT_CHECK_ROWS);

        options.add(HdfsOptions.FILTER_REGEX);
        options.add(HdfsOptions.FIELD_DELIMITER);
        options.add(HdfsOptions.ENABLE_DICTIONARY);

        options.add(HiveOptions.USERNAME);
        options.add(HiveOptions.PASSWORD);
        options.add(HiveOptions.PARTITION_TYPE);
        options.add(HiveOptions.PARTITION);

        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validateExcept("properties.");

        // 3.封装参数
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        HiveConfig hiveConf = getHiveConf(config);
        hiveConf.setHadoopConfig(
                HdfsOptions.getHadoopConfig(context.getCatalogTable().getOptions()));
        buildTablesColumn(hiveConf, config.get(HiveOptions.TABLE_NAME), physicalSchema);
        return new HiveDynamicTableSink(hiveConf, physicalSchema);
    }

    private HiveConfig getHiveConf(ReadableConfig config) {
        HiveConfig hiveConf = new HiveConfig();
        hiveConf.setParallelism(config.get(SinkOptions.SINK_PARALLELISM));

        hiveConf.setPath(config.get(BaseFileOptions.PATH));
        hiveConf.setFileName(config.get(BaseFileOptions.FILE_NAME));
        hiveConf.setWriteMode(config.get(BaseFileOptions.WRITE_MODE));
        hiveConf.setCompress(config.get(BaseFileOptions.COMPRESS));
        hiveConf.setEncoding(config.get(BaseFileOptions.ENCODING));
        hiveConf.setMaxFileSize(config.get(BaseFileOptions.MAX_FILE_SIZE));
        hiveConf.setNextCheckRows(config.get(BaseFileOptions.NEXT_CHECK_ROWS));

        hiveConf.setDefaultFS(config.get(HdfsOptions.DEFAULT_FS));
        hiveConf.setFileType(config.get(HdfsOptions.FILE_TYPE));
        hiveConf.setFilterRegex(config.get(HdfsOptions.FILTER_REGEX));
        hiveConf.setFieldDelimiter(config.get(HdfsOptions.FIELD_DELIMITER));
        hiveConf.setEnableDictionary(config.get(HdfsOptions.ENABLE_DICTIONARY));

        hiveConf.setJdbcUrl(config.get(HiveOptions.JDBC_URL));
        hiveConf.setUsername(config.get(HiveOptions.USERNAME));
        hiveConf.setPassword(config.get(HiveOptions.PASSWORD));
        hiveConf.setPartitionType(config.get(HiveOptions.PARTITION_TYPE));
        hiveConf.setPartition(config.get(HiveOptions.PARTITION));

        return hiveConf;
    }

    private void buildTablesColumn(HiveConfig hiveConf, String tableName, TableSchema tableSchema) {
        RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();
        String[] fieldNames = tableSchema.getFieldNames();
        List<Map<String, String>> list = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            Map<String, String> map = new LinkedHashMap<>();
            map.put("key", fieldNames[i]);
            LogicalType logicalType = rowType.getTypeAt(i);

            if (logicalType instanceof BinaryType) {
                map.put("type", "binary");
            } else if (logicalType instanceof TimestampType) {
                map.put("type", "timestamp");
            } else {
                map.put("type", logicalType.asSummaryString());
            }
            list.add(map);
        }
        String tablesColumn = JsonUtil.toJson(Collections.singletonMap(tableName, list));
        hiveConf.setTablesColumn(tablesColumn);
        hiveConf.setTableName(tableName);
        hiveConf.setAutoCreateTable(false);
    }
}
