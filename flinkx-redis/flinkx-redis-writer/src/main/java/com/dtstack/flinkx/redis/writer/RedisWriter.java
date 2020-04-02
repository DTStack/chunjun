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

package com.dtstack.flinkx.redis.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.redis.DataMode;
import com.dtstack.flinkx.redis.DataType;
import com.dtstack.flinkx.redis.JedisUtil;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static com.dtstack.flinkx.redis.RedisConfigKeys.*;

/**
 * The writer plugin for redis database
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class RedisWriter extends BaseDataWriter {

    private String hostPort;

    private int batchSize;

    private String password;

    private int database = 0;

    private List<Integer> keyIndexes = new ArrayList<>();

    private String keyFieldDelimiter;

    private String dateFormat;

    private long expireTime;

    private int timeout;

    private DataType type;

    private DataMode dataMode;

    private String valueFieldDelimiter;

    public RedisWriter(DataTransferConfig config) {
        super(config);

        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        hostPort = writerConfig.getParameter().getStringVal(KEY_HOST_PORT);
        batchSize = writerConfig.getParameter().getIntVal(KEY_BATCH_SIZE,1);
        password = writerConfig.getParameter().getStringVal(KEY_PASSWORD);
        database = writerConfig.getParameter().getIntVal(KEY_DB,0);
        keyFieldDelimiter = (String)writerConfig.getParameter().getVal(KEY_KEY_FIELD_DELIMITER,JedisUtil.DELIMITER);
        dateFormat = writerConfig.getParameter().getStringVal(KEY_DATE_FORMAT);
        expireTime = writerConfig.getParameter().getLongVal(KEY_EXPIRE_TIME,0);
        timeout = writerConfig.getParameter().getIntVal(KEY_TIMEOUT,JedisUtil.TIMEOUT);
        type = DataType.getDataType(writerConfig.getParameter().getStringVal(KEY_TYPE));
        dataMode = DataMode.getDataMode(writerConfig.getParameter().getStringVal(KEY_MODE));
        valueFieldDelimiter = (String)writerConfig.getParameter().getVal(KEY_VALUE_FIELD_DELIMITER,JedisUtil.DELIMITER);

        for (Object item : (List<Object>) writerConfig.getParameter().getVal(KEY_KEY_INDEXES)) {
            if (item instanceof Double){
                keyIndexes.add(((Double)item).intValue());
            } else if(item instanceof Float){
                keyIndexes.add(((Float)item).intValue());
            } else if(item instanceof String) {
                keyIndexes.add(Integer.valueOf(String.valueOf(item)));
            } else {
                keyIndexes.add((Integer) item);
            }
        }
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        RedisOutputFormatBuilder builder = new RedisOutputFormatBuilder();

        builder.setHostPort(hostPort);
        builder.setPassword(password);
        builder.setDatabase(database);
        builder.setKeyIndexes(keyIndexes);
        builder.setKeyFieldDelimiter(keyFieldDelimiter);
        builder.setDateFormat(dateFormat);
        builder.setExpireTime(expireTime);
        builder.setTimeout(timeout);
        builder.setType(type);
        builder.setDataMode(dataMode);
        builder.setValueFieldDelimiter(valueFieldDelimiter);

        builder.setMonitorUrls(monitorUrls);
        builder.setErrors(errors);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);
        builder.setBatchInterval(batchSize);

        return createOutput(dataSet, builder.finish());
    }
}
