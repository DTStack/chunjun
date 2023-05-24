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

package com.dtstack.chunjun.connector.hudi.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.hudi.converter.HudiRawTypeConvertor;
import com.dtstack.chunjun.connector.hudi.converter.HudiRowDataConvertorMap;
import com.dtstack.chunjun.connector.hudi.converter.HudiRowDataMapping;
import com.dtstack.chunjun.connector.hudi.utils.HudiConfigUtil;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieTableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HudiSourceFactory extends SourceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(HudiSourceFactory.class);
    private SyncConfig syncConf;
    private Map<String, String> hudiConfig;
    private Configuration flinkConf;

    public HudiSourceFactory(SyncConfig syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        // Get hudiConfig from reader config.
        hudiConfig = (Map<String, String>) syncConf.getReader().getParameter().get("hudiConfig");
        flinkConf = Configuration.fromMap(hudiConfig);
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return HudiRowDataMapping::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        List<FieldConfig> fieldList = syncConfig.getReader().getFieldList();
        ResolvedSchema schema = TableUtil.createTableSchema(fieldList, getRawTypeMapper());
        String partitions = flinkConf.getString(FlinkOptions.PARTITION_PATH_FIELD, "");
        String[] partition = partitions.split(",");
        String keys = flinkConf.get(FlinkOptions.RECORD_KEY_FIELD);
        List<String> partitionList =
                partition[0].equals("") ? Collections.emptyList() : Arrays.asList(partition);
        // Some check on flink config and hudi schema.
        HudiConfigUtil.sanityCheck(flinkConf, schema);

        Path path =
                new Path(
                        flinkConf
                                .getOptional(FlinkOptions.PATH)
                                .orElseThrow(
                                        () ->
                                                new ValidationException(
                                                        "Option [path] should not be empty.")));
        // Init hudi config on flink config.
        HudiConfigUtil.setupTableOptions(flinkConf.getString(FlinkOptions.PATH), flinkConf, schema);
        String tableName = flinkConf.getString(FlinkOptions.TABLE_NAME);
        if (StringUtils.isBlank(tableName)) {
            throw new RuntimeException("必要配置项：" + FlinkOptions.TABLE_NAME + " 没有配置！");
        }
        HudiConfigUtil.setupConfOptions(flinkConf, tableName, schema, keys, partitions);

        // Generate hudi source provider.
        HoodieTableSource hoodieTableSource =
                new HoodieTableSource(
                        schema,
                        path,
                        partitionList,
                        flinkConf.getString(FlinkOptions.PARTITION_DEFAULT_NAME),
                        flinkConf);

        HudiRawTypeConvertor convertor = new HudiRawTypeConvertor(fieldList);

        DataStreamScanProvider sourceProvider =
                (DataStreamScanProvider) (hoodieTableSource.getScanRuntimeProvider(null));
        return sourceProvider.produceDataStream(env).map(new HudiRowDataConvertorMap(convertor));
    }
}
