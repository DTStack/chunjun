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

package com.dtstack.flinkx.s3.reader;

import com.amazonaws.services.s3.AmazonS3;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.DataReaderFactory;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.s3.S3SimpleObject;
import com.dtstack.flinkx.s3.S3Util;
import com.dtstack.flinkx.s3.S3Config;
import com.dtstack.flinkx.s3.format.RegexUtil;
import com.dtstack.flinkx.s3.format.S3InputFormat;
import com.dtstack.flinkx.s3.format.S3InputFormatBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Instance by DataReaderFactory{@link DataReaderFactory}
 * <p>
 * company www.dtstack.com
 *
 * @author jier
 */
public class S3Reader extends BaseDataReader {
    private static final Logger LOG = LoggerFactory.getLogger(S3Reader.class);


    private S3Config s3Config;
    private List<MetaColumn> metaColumns;

    public S3Reader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();

        try {
            String s3json = objectMapper.writeValueAsString(readerConfig.getParameter().getAll());
            s3Config = objectMapper.readValue(s3json, S3Config.class);
        } catch (Exception e) {
            throw new RuntimeException("解析S3Config配置出错:", e);
        }


        List columns = readerConfig.getParameter().getColumn();
        metaColumns = MetaColumn.getMetaColumns(columns, false);
    }

    @Override
    public DataStream<Row> readData() {
        S3InputFormatBuilder builder = new S3InputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setS3Config(s3Config);
        builder.setMetaColumn(metaColumns);
        builder.setRestoreConfig(restoreConfig);
        BaseRichInputFormat inputFormat = builder.finish();
        resolveObjects((S3InputFormat) inputFormat);
        return createInput(inputFormat);
    }

    /**
     * 根据配置的 object 去获取所有需要读取的 object
     *
     * @param s3InputFormat
     */
    public void resolveObjects(S3InputFormat s3InputFormat) {
        S3Config s3Config = s3InputFormat.getS3Config();
        String bucket = s3Config.getBucket();
        Set<S3SimpleObject> resolved = new HashSet<>();
        AmazonS3 amazonS3 = S3Util.initS3(s3Config);
        for (String key : s3Config.getObject()) {
            if (StringUtils.isNotBlank(key)) {
                RegexUtil.initializeOrReset(key);
                String prefix = RegexUtil.getCommonPrefix();
                if (StringUtils.isBlank(prefix)) {
                    //公共前缀为空，则说明 key 为 */xxx/yyy... ,前缀不确定,需要查询出所有 key，再利用正则表达式去匹配
                    List<String> subObjects = S3Util.listObjectsKey(amazonS3, bucket);
                    for (String subObject : subObjects) {
                        //遍历所有 key，去匹配正则表达式
                        if (RegexUtil.match(subObject)) {
                            //如果匹配正则表达式，则获取其元数据信息
                            S3SimpleObject s3SimpleObject = S3Util.getS3SimpleObject(amazonS3, bucket, subObject);
                            resolved.add(s3SimpleObject);
                        }
                    }
                } else {
                    if (key.equals(prefix)) {
                        // 该 key 不是正则表达式，是唯一的
                        if (S3Util.doesObjectExist(amazonS3, bucket, key)) {
                            S3SimpleObject s3SimpleObject = S3Util.getS3SimpleObject(amazonS3, bucket, key);
                            resolved.add(s3SimpleObject);
                        }
                    } else if (key.startsWith(prefix)) {
                        //该 key 是正则表达式，利用公共前缀去查询，再利用正则表达式去匹配
                        List<String> subObjects = S3Util.listObjectsKeyByPrefix(amazonS3, bucket, prefix);
                        for (String subObject : subObjects) {
                            //遍历所有 key，去匹配正则表达式
                            if (RegexUtil.match(subObject)) {
                                //如果匹配正则表达式，则获取其元数据信息
                                S3SimpleObject s3SimpleObject = S3Util.getS3SimpleObject(amazonS3, bucket, subObject);
                                resolved.add(s3SimpleObject);
                            }
                        }
                    }
                }

            }
        }
        List<S3SimpleObject> distinct = new ArrayList<>(resolved);
        LOG.info("match object is[{}]", distinct.stream().map(S3SimpleObject::getKey)
                .collect(Collectors.joining(",")));
        s3InputFormat.setObjects(distinct);
    }
}
