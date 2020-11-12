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
package com.dtstack.flinkx.emqx.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.emqx.format.EmqxOutputFormatBuilder;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

import static com.dtstack.flinkx.emqx.EmqxConfigKeys.KEY_BROKER;
import static com.dtstack.flinkx.emqx.EmqxConfigKeys.KEY_IS_CLEAN_SESSION;
import static com.dtstack.flinkx.emqx.EmqxConfigKeys.KEY_PASSWORD;
import static com.dtstack.flinkx.emqx.EmqxConfigKeys.KEY_QOS;
import static com.dtstack.flinkx.emqx.EmqxConfigKeys.KEY_TOPIC;
import static com.dtstack.flinkx.emqx.EmqxConfigKeys.KEY_USERNAME;

/**
 * Date: 2020/02/12
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class EmqxWriter extends BaseDataWriter {
    private String broker;
    private String topic;
    private String username;
    private String password;
    private boolean isCleanSession;
    private int qos;

    public EmqxWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        broker = writerConfig.getParameter().getStringVal(KEY_BROKER);
        topic = writerConfig.getParameter().getStringVal(KEY_TOPIC);
        username = writerConfig.getParameter().getStringVal(KEY_USERNAME);
        password = writerConfig.getParameter().getStringVal(KEY_PASSWORD);
        isCleanSession = writerConfig.getParameter().getBooleanVal(KEY_IS_CLEAN_SESSION, true);
        qos = writerConfig.getParameter().getIntVal(KEY_QOS, 2);
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        EmqxOutputFormatBuilder builder = new EmqxOutputFormatBuilder();
        builder.setBroker(broker);
        builder.setTopic(topic);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setCleanSession(isCleanSession);
        builder.setQos(qos);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);
        return createOutput(dataSet, builder.finish());
    }
}
