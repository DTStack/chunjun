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
import com.dtstack.flinkx.config.DirtyConfig;
import com.dtstack.flinkx.plugin.PluginLoader;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
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

    protected PluginLoader pluginLoader;

    protected List<String> srcCols = new ArrayList<>();

    /**
     * reuse hadoopConfig for metric
     */
    protected Map<String, String> hadoopConfig;

    public List<String> getSrcCols() {
        return srcCols;
    }

    public void setSrcCols(List<String> srcCols) {
        this.srcCols = srcCols;
    }

    public PluginLoader getPluginLoader() {
        return pluginLoader;
    }


    public void setPluginLoader(PluginLoader pluginLoader) {
        this.pluginLoader = pluginLoader;
    }

    protected List<String> jarNameList = new ArrayList<>();

    protected DataReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        this.env = env;
        this.numPartitions = config.getJob().getSetting().getSpeed().getChannel();
        this.bytes = config.getJob().getSetting().getSpeed().getBytes();
        this.monitorUrls = config.getMonitorUrls();

        DirtyConfig dirtyConfig = config.getJob().getSetting().getDirty();
        if (dirtyConfig != null) {
            Map<String, String> hadoopConfig = dirtyConfig.getHadoopConfig();
            if (hadoopConfig != null) {
                this.hadoopConfig = hadoopConfig;
            }
        }
    }

    public abstract DataStream<Row> readData();

    protected DataStream<Row> createInput(InputFormat inputFormat, String sourceName) {
        Preconditions.checkNotNull(sourceName);
        Preconditions.checkNotNull(inputFormat);
        TypeInformation typeInfo = TypeExtractor.getInputFormatTypes(inputFormat);
        InputFormatSourceFunction function = new InputFormatSourceFunction(inputFormat, typeInfo);
        return env.addSource(function, sourceName, typeInfo);
    }

}
