/**
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
import com.dtstack.flinkx.config.RestoreConfig;
import com.dtstack.flinkx.config.DirtyConfig;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.DtInputFormatSourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Abstract specification of Reader Plugin
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class DataReader {

    protected StreamExecutionEnvironment env;

    protected int numPartitions = 1;

    protected long bytes = Long.MAX_VALUE;

    protected String monitorUrls;

    protected RestoreConfig restoreConfig;

    protected List<String> srcCols = new ArrayList<>();

    protected long exceptionIndex;

    /**
     * reuse hadoopConfig for metric
     */
    protected Map<String, Object> hadoopConfig;

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

    protected DataReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        this.env = env;
        this.numPartitions = config.getJob().getSetting().getSpeed().getChannel();
        this.bytes = config.getJob().getSetting().getSpeed().getBytes();
        this.monitorUrls = config.getMonitorUrls();
        this.restoreConfig = config.getJob().getSetting().getRestoreConfig();
        this.exceptionIndex = config.getJob().getContent().get(0).getReader().getParameter().getLongVal("exceptionIndex",0);

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

    public abstract DataStream<Row> readData();

    protected DataStream<Row> createInput(InputFormat inputFormat, String sourceName) {
        Preconditions.checkNotNull(sourceName);
        Preconditions.checkNotNull(inputFormat);
        TypeInformation typeInfo = TypeExtractor.getInputFormatTypes(inputFormat);
        DtInputFormatSourceFunction function = new DtInputFormatSourceFunction(inputFormat, typeInfo);
        return env.addSource(function, sourceName, typeInfo);
    }

}
