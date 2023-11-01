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
package com.dtstack.chunjun.connector.hdfs.table;

import com.dtstack.chunjun.connector.hdfs.config.HdfsConfig;
import com.dtstack.chunjun.connector.hdfs.options.HdfsOptions;
import com.dtstack.chunjun.connector.hdfs.sink.HdfsDynamicTableSink;
import com.dtstack.chunjun.connector.hdfs.source.HdfsDynamicTableSource;
import com.dtstack.chunjun.source.options.SourceOptions;
import com.dtstack.chunjun.table.options.BaseFileOptions;
import com.dtstack.chunjun.table.options.SinkOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.commons.lang3.StringEscapeUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HdfsDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "hdfs-x";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HdfsOptions.DEFAULT_FS);
        options.add(HdfsOptions.FILE_TYPE);
        options.add(HdfsOptions.PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SourceOptions.SCAN_PARALLELISM);
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
        options.add(HdfsOptions.SINK_COMMIT_FINISHED_FILE_NAME);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validateExcept("properties.");

        // 3.封装参数
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        HdfsConfig hdfsConfig = getHdfsConfig(config);
        List<String> partitionKeys = context.getCatalogTable().getPartitionKeys();
        hdfsConfig.setParallelism(config.get(SourceOptions.SCAN_PARALLELISM));
        hdfsConfig.setHadoopConfig(
                HdfsOptions.getHadoopConfig(context.getCatalogTable().getOptions()));

        return new HdfsDynamicTableSource(hdfsConfig, resolvedSchema, partitionKeys);
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
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        HdfsConfig hdfsConfig = getHdfsConfig(config);
        hdfsConfig.setParallelism(config.get(SinkOptions.SINK_PARALLELISM));
        hdfsConfig.setHadoopConfig(
                HdfsOptions.getHadoopConfig(context.getCatalogTable().getOptions()));

        return new HdfsDynamicTableSink(hdfsConfig, resolvedSchema);
    }

    /**
     * initialize HdfsConfig
     *
     * @param config
     * @return
     */
    private HdfsConfig getHdfsConfig(ReadableConfig config) {
        HdfsConfig hdfsConfig = new HdfsConfig();

        hdfsConfig.setPath(config.get(BaseFileOptions.PATH));
        hdfsConfig.setFileName(config.get(BaseFileOptions.FILE_NAME));
        hdfsConfig.setWriteMode(config.get(BaseFileOptions.WRITE_MODE));
        hdfsConfig.setCompress(config.get(BaseFileOptions.COMPRESS));
        hdfsConfig.setEncoding(config.get(BaseFileOptions.ENCODING));
        hdfsConfig.setMaxFileSize(config.get(BaseFileOptions.MAX_FILE_SIZE));
        hdfsConfig.setNextCheckRows(config.get(BaseFileOptions.NEXT_CHECK_ROWS));
        hdfsConfig.setJobIdentifier(config.get(BaseFileOptions.JOB_IDENTIFIER));

        hdfsConfig.setDefaultFS(config.get(HdfsOptions.DEFAULT_FS));
        hdfsConfig.setFileType(config.get(HdfsOptions.FILE_TYPE));
        hdfsConfig.setFilterRegex(config.get(HdfsOptions.FILTER_REGEX));
        hdfsConfig.setFieldDelimiter(
                StringEscapeUtils.unescapeJava(config.get(HdfsOptions.FIELD_DELIMITER)));
        hdfsConfig.setEnableDictionary(config.get(HdfsOptions.ENABLE_DICTIONARY));
        hdfsConfig.setFinishedFileName(config.get(HdfsOptions.SINK_COMMIT_FINISHED_FILE_NAME));

        return hdfsConfig;
    }
}
