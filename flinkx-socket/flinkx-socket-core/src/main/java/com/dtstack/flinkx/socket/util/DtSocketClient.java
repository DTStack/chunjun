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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.types.Row;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

/** 采用netty实现Socket Client
 * Date: 2019/09/20
 * Company: www.dtstack.com
 *
 * @author kunni
 */

public class DtSocketClient implements Closeable, Serializable {

    protected String host;
    protected String port;

    protected EventLoopGroup group;
    protected DtChannelInitializer initializer = new DtChannelInitializer();

    public DtSocketClient(String host, String port){
        this.host = host;
        this.port = port;
    }

    //开启监听
    public void start() throws IOException {
        group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(initializer);
            bootstrap.connect(host, Integer.parseInt(port)).sync();
        }catch (Exception e){
            throw new IOException(e);
        }
    }

    @Override
    public void close() {
        group.shutdownGracefully();
    }

    public void setInitializer(DtChannelInitializer initializer) {
        this.initializer = initializer;
    }

}
