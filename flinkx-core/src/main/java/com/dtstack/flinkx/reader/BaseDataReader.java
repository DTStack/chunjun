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

package com.dtstack.flinkx.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.LogConfig;
import com.dtstack.flinkx.config.RestoreConfig;
import com.dtstack.flinkx.config.DirtyConfig;
import com.dtstack.flinkx.config.TestConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.dtstack.flinkx.streaming.api.functions.source.DtInputFormatSourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Abstract specification of Reader Plugin
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class BaseDataReader {

    protected StreamExecutionEnvironment env;

    protected int numPartitions = 1;

    protected long bytes = Long.MAX_VALUE;

    protected String monitorUrls;

    protected RestoreConfig restoreConfig;

    protected LogConfig logConfig;

    protected TestConfig testConfig;

    protected List<String> srcCols = new ArrayList<>();

    protected long exceptionIndex;

    protected DataTransferConfig dataTransferConfig;

    /**
     * reuse hadoopConfig for metric
     */
    protected Map<String, Object> hadoopConfig;

    protected static ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public int getNumPartitions() {
        return numPartitions;
    }

    public RestoreConfig getRestoreConfig() {
        return restoreConfig;
    }

    public List<String> getSrcCols() {
        return srcCols;
    }

    public void setSrcCols(List<String> srcCols) {
        this.srcCols = srcCols;
    }

    protected BaseDataReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        this.env = env;
        this.dataTransferConfig = config;
        this.numPartitions = Math.max(config.getJob().getSetting().getSpeed().getChannel(),
                config.getJob().getSetting().getSpeed().getReaderChannel());
        this.bytes = config.getJob().getSetting().getSpeed().getBytes();
        this.monitorUrls = config.getMonitorUrls();
        this.restoreConfig = config.getJob().getSetting().getRestoreConfig();
        this.testConfig = config.getJob().getSetting().getTestConfig();
        this.logConfig = config.getJob().getSetting().getLogConfig();

        DirtyConfig dirtyConfig = config.getJob().getSetting().getDirty();
        if (dirtyConfig != null) {
            Map<String, Object> hadoopConfig = dirtyConfig.getHadoopConfig();
            if (hadoopConfig != null) {
                this.hadoopConfig = hadoopConfig;
            }
        }

        if (restoreConfig.isStream()){
            return;
        }

        if(restoreConfig.isRestore()){
            List columns = config.getJob().getContent().get(0).getReader().getParameter().getColumn();
            MetaColumn metaColumn = MetaColumn.getMetaColumn(columns, restoreConfig.getRestoreColumnName());
            if(metaColumn == null){
                throw new RuntimeException("Can not find restore column from json with column name:" + restoreConfig.getRestoreColumnName());
            }
            restoreConfig.setRestoreColumnIndex(metaColumn.getIndex());
            restoreConfig.setRestoreColumnType(metaColumn.getType());
        }
    }

    /**
     * Build the read data flow object
     *
     * @return DataStream
     */
    public abstract DataStream<Row> readData();

    @SuppressWarnings("unchecked")
    protected DataStream<Row> createInput(InputFormat inputFormat, String sourceName) {
        Preconditions.checkNotNull(sourceName);
        Preconditions.checkNotNull(inputFormat);
        TypeInformation typeInfo = TypeExtractor.getInputFormatTypes(inputFormat);
        DtInputFormatSourceFunction function = new DtInputFormatSourceFunction(inputFormat, typeInfo);
        return env.addSource(function, sourceName, typeInfo);
    }

    protected DataStream<Row> createInput(InputFormat inputFormat) {
        return createInput(inputFormat,this.getClass().getSimpleName().toLowerCase());
    }

}
