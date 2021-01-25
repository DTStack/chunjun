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
package com.dtstack.flinkx.metadatahive2.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.metadatahive2.inputformat.Metadatahive2InputFormat;
import com.dtstack.flinkx.metadatahive2.inputformat.Metadatehive2InputFormatBuilder;
import com.dtstack.metadata.rdb.builder.MetadatardbBuilder;
import com.dtstack.metadata.rdb.reader.MetadatardbReader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

import static com.dtstack.flinkx.metatdata.hive2.core.util.Hive2MetaDataCons.DRIVER_NAME;
import static com.dtstack.flinkx.metatdata.hive2.core.util.Hive2MetaDataCons.KEY_HADOOP_CONFIG;

/**
 * @author : tiezhu
 * @date : 2020/3/9
 */
public class Metadatahive2Reader extends MetadatardbReader {

    public Metadatahive2Reader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        hadoopConfig = (Map<String, Object>) readerConfig.getParameter().getVal(KEY_HADOOP_CONFIG);
    }



    @Override
    public MetadatardbBuilder createBuilder() {
        Metadatehive2InputFormatBuilder builder = new Metadatehive2InputFormatBuilder(new Metadatahive2InputFormat());
        builder.setHadoopConfig(hadoopConfig);
        return builder;
    }

    @Override
    public String getDriverName() {
        return DRIVER_NAME;
    }
}
