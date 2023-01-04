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

package com.dtstack.chunjun.connector.socket.client;

import com.dtstack.chunjun.util.ExceptionUtil;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.Closeable;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.concurrent.SynchronousQueue;

import static com.dtstack.chunjun.connector.socket.inputformat.SocketInputFormat.KEY_EXIT0;

@Slf4j
public class DtSocketClient implements Closeable, Serializable {

    private static final long serialVersionUID = 5485855168344870216L;
    protected String host;
    protected int port;
    protected String encoding = "UTF-8";

    protected String codeC;
    protected EventLoopGroup group = new NioEventLoopGroup();
    protected SynchronousQueue<RowData> queue;

    public Channel channel;

    public DtSocketClient(String host, int port, SynchronousQueue<RowData> queue) {
        this.host = host;
        this.port = port;
        this.queue = queue;
    }

    public void start() {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            public void initChannel(SocketChannel ch) {
                                ch.pipeline()
                                        .addLast(
                                                "decoder",
                                                new StringDecoder(Charset.forName(encoding)));
                                ch.pipeline()
                                        .addLast(
                                                "encoder",
                                                new StringEncoder(Charset.forName(encoding)));
                                ch.pipeline().addLast(new DtClientHandler(queue, codeC, encoding));
                            }
                        });
        channel =
                bootstrap
                        .connect(host, port)
                        .addListener(
                                future -> {
                                    if (future.isSuccess()) {
                                        log.info("connect [{}:{}] success", host, port);
                                    } else {
                                        String error =
                                                String.format("connect [%s:%d] failed", host, port);
                                        try {
                                            queue.put(GenericRowData.of(KEY_EXIT0 + error));
                                        } catch (InterruptedException ex) {
                                            log.error(ExceptionUtil.getErrorMessage(ex));
                                        }
                                    }
                                })
                        .channel();
    }

    public void setCodeC(String codeC) {
        this.codeC = codeC;
    }

    public void setEncoding(String encoding) {
        if (StringUtils.isNotEmpty(encoding)) {
            this.encoding = encoding;
        }
    }

    @Override
    public void close() {
        log.error("close channel!!! ");
        if (channel != null) {
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
    }
}
