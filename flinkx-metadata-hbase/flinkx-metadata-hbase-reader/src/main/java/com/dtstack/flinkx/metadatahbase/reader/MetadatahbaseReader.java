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

package com.dtstack.flinkx.metadatahbase.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.metadata.inputformat.MetadataInputFormatBuilder;
import com.dtstack.flinkx.metadata.reader.MetadataReader;
import com.dtstack.flinkx.metadatahbase.inputformat.MetadatahbaseInputformat;
import com.dtstack.flinkx.metadatahbase.inputformat.MetadatahbaseInputformatBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

import static com.dtstack.flinkx.metadatahbase.constants.HbaseCons.KEY_HADOOP_CONFIG;

/**
 * 读取hbase config并进行配置
 * @author kunni@dtstack.com
 */
public class MetadatahbaseReader extends MetadataReader {

    private Map<String, Object> hadoopConfig;

    @SuppressWarnings("unchecked")
    public MetadatahbaseReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        hadoopConfig = (Map<String, Object>) readerConfig.getParameter().getVal(KEY_HADOOP_CONFIG);
    }

    @Override
    protected MetadataInputFormatBuilder getBuilder(){
        MetadatahbaseInputformatBuilder builder = new MetadatahbaseInputformatBuilder(new MetadatahbaseInputformat());
        builder.setHadoopConfig(hadoopConfig);
        return builder;
    }

}
