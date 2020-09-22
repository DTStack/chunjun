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

package com.dtstack.flinkx.socket.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.BaseDataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.Map;

import static com.dtstack.flinkx.socket.constants.SocketCons.KEY_BINARY_ARRAY_DECODER;
import static com.dtstack.flinkx.socket.constants.SocketCons.KEY_BYTE_BUF_DECODER;
import static com.dtstack.flinkx.socket.constants.SocketCons.KEY_PROPERTIES;
import static com.dtstack.flinkx.socket.constants.SocketCons.KEY_SERVER;

/** 读取用户传入参数
 *
 * @author by kunni@dtstack.com
 * @Date 2020/09/18
 */

public class SocketReader extends BaseDataReader {

    protected String server;

    protected String byteBufDecoder;

    protected String binaryArrayDecoder;

    protected Map<String, Object> properties;

    protected SocketReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        server = readerConfig.getParameter().getStringVal(KEY_SERVER);
        byteBufDecoder = readerConfig.getParameter().getStringVal(KEY_BYTE_BUF_DECODER);
        binaryArrayDecoder = readerConfig.getParameter().getStringVal(KEY_BINARY_ARRAY_DECODER);
        properties = (Map<String, Object>) readerConfig.getParameter().getVal(KEY_PROPERTIES);
    }

    @Override
    public DataStream<Row> readData() {
        SocketBuilder socketBuilder = new SocketBuilder();
        socketBuilder.setServer(server);
        socketBuilder.setByteBufDecoder(byteBufDecoder);
        socketBuilder.setBinaryArrayDecoder(binaryArrayDecoder);
        socketBuilder.setProperties(properties);
        return createInput(socketBuilder.finish());
    }
}
