/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.metadata.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.metadata.builder.MetadataBaseBuilder;
import com.dtstack.flinkx.metadata.core.util.BaseCons;
import com.dtstack.flinkx.reader.BaseDataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

/**
 * @author kunni@dtstack.com
 */
abstract public class MetaDataBaseReader extends BaseDataReader {

    protected ReaderConfig.ParameterConfig params;

    protected List<Map<String, Object>> originalJob;

    @SuppressWarnings("unchecked")
    protected MetaDataBaseReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        params = config.getJob().getContent().get(0).getReader().getParameter();
        originalJob = (List<Map<String, Object>>) params.getVal(BaseCons.KEY_DB_LIST);
    }

    @Override
    public DataStream<Row> readData() {
        MetadataBaseBuilder builder = createBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setOriginalJob(originalJob);
        return createInput(builder.finish());
    }

    /**
     * 子类实现，在builder中给inputFormat 传值
     * @return 构建完成的builder
     */
    abstract public MetadataBaseBuilder createBuilder();
}
