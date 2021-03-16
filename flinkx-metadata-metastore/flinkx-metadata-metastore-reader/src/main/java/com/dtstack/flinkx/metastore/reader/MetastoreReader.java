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

package com.dtstack.flinkx.metastore.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.metadata.builder.MetadataBaseBuilder;
import com.dtstack.flinkx.metastore.builder.MetaStoreInputFormatBuilder;
import com.dtstack.flinkx.metastore.inputformat.MetaStoreInputFormat;
import com.dtstack.flinkx.metadata.reader.MetaDataBaseReader;
import com.dtstack.flinkx.metatdata.hive.core.util.HiveMetaDataCons;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-04 14:30
 * @Description:
 */
public class MetastoreReader extends MetaDataBaseReader {


    public MetastoreReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        hadoopConfig = (Map<String, Object>) readerConfig.getParameter().getVal(HiveMetaDataCons.KEY_HADOOP_CONFIG);
    }


    @Override
    public MetadataBaseBuilder createBuilder() {
        MetaStoreInputFormatBuilder metadataBuilder = new MetaStoreInputFormatBuilder(new MetaStoreInputFormat());
        metadataBuilder.setHadoopConfig(hadoopConfig);
        return metadataBuilder;
    }
}
