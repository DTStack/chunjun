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

package com.dtstack.flinkx.socket.util;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.FixedLengthFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.Map;

import static com.dtstack.flinkx.socket.constants.SocketCons.DEFAULT_IDLE_STATE_HANDLER_THREAD;

/**
 * Date: 2019/09/20
 * Company: www.dtstack.com
 *
 * @author kunni
 */

public class DtChannelInitializer extends ChannelInitializer<SocketChannel> {

    protected Map<String, String> properties;

    private final EventExecutorGroup idleExecutorGroup = new DefaultEventExecutorGroup(DEFAULT_IDLE_STATE_HANDLER_THREAD);

    public DtChannelInitializer(){

    }

    @Override
    protected void initChannel(SocketChannel channel){
        ChannelPipeline pipeLine = channel.pipeline();
        String keepAliveHandler = "keep-alive-handler";
        pipeLine.addLast(idleExecutorGroup, keepAliveHandler, new IdleStateHandler(60*15, 5, 0));
    }

    /**
     *
     * @return 编解码器
     */
    protected ByteToMessageDecoder getByteToMessageDecoder() {
        return new FixedLengthFrameDecoder(1);
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }


}
