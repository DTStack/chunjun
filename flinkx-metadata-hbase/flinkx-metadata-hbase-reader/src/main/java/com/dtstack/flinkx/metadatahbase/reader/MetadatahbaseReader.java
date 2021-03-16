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
import com.dtstack.flinkx.metadata.builder.MetadataBaseBuilder;
import com.dtstack.flinkx.metadata.reader.MetaDataBaseReader;
import com.dtstack.flinkx.metadatahbase.inputformat.MetadatahbaseInputFormat;
import com.dtstack.flinkx.metadatahbase.inputformat.MetadatahbaseInputFormatBuilder;
import com.dtstack.flinkx.metadatahbase.util.HbaseCons;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.HConstants;

import java.util.Map;

import static com.dtstack.flinkx.metadatahbase.util.HbaseCons.KEY_HADOOP_CONFIG;
import static com.dtstack.flinkx.metadatahbase.util.HbaseCons.KEY_PATH;
import static com.dtstack.flinkx.util.ZkHelper.DEFAULT_PATH;

/**
 * 读取hbase config并进行配置
 * @author kunni@dtstack.com
 */
public class MetadatahbaseReader extends MetaDataBaseReader {

    /**用于连接hbase的配置*/
    private Map<String, Object> hadoopConfig;

    /**hbase znode路径*/
    private String path;

    /**zookeeper地址端口*/
    private String zooKeeperUrl;


    @SuppressWarnings("unchecked")
    public MetadatahbaseReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        zooKeeperUrl = config.getJob().getContent()
                .get(0).getReader().getParameter().getStringVal(HbaseCons.ZOOKEEPER_URL);
        hadoopConfig = (Map<String, Object>) config.getJob().getContent()
                .get(0).getReader().getParameter().getVal(KEY_HADOOP_CONFIG);
        if (!hadoopConfig.containsKey(HConstants.ZOOKEEPER_QUORUM)) {
            hadoopConfig.put(HConstants.ZOOKEEPER_QUORUM, zooKeeperUrl);
        }
        path = config.getJob().getContent().get(0).getReader()
                .getParameter().getStringVal(KEY_PATH, DEFAULT_PATH);
        if (!hadoopConfig.containsKey(HConstants.ZOOKEEPER_ZNODE_PARENT)) {
            hadoopConfig.put(HConstants.ZOOKEEPER_ZNODE_PARENT, path);
        }

    }

    @Override
    public MetadataBaseBuilder createBuilder() {
        MetadatahbaseInputFormatBuilder builder = new MetadatahbaseInputFormatBuilder(new MetadatahbaseInputFormat());
        builder.setHadoopConfig(hadoopConfig);
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setPath(path);
        return builder;
    }


}
