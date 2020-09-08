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

package com.dtstack.flinkx.websocket.format;

import com.dtstack.flinkx.decoder.IDecode;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.flink.types.Row;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.concurrent.SynchronousQueue;

/**
 * 基于webSocket的netty客户端
 * @Company: www.dtstack.com
 * @author kunni
 */

public class WebSocketClient {

    private String serverUrl;

    private EventLoopGroup group = new NioEventLoopGroup();

    private Bootstrap boot = new Bootstrap();

    WebSocketClientHandler webSocketClientHandler;

    public WebSocketClient(SynchronousQueue<Row> queue, String serverUrl, IDecode decoder) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        webSocketClientHandler = new WebSocketClientHandler(queue, decoder);
        this.serverUrl = serverUrl;
    }

    public void start() throws Exception {
        boot.option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .group(group)
                .handler(new LoggingHandler(LogLevel.INFO))
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        pipeline.addLast("http-codec",new HttpClientCodec());
                        pipeline.addLast("aggregator",new HttpObjectAggregator(1024*1024*10));
                        pipeline.addLast("hookedHandler", webSocketClientHandler);
                    }
                });
        URI webSocketURI = new URI(serverUrl);
        HttpHeaders httpHeaders = new DefaultHttpHeaders();
        //进行握手
        WebSocketClientHandshaker handsShaker = WebSocketClientHandshakerFactory.newHandshaker(webSocketURI, WebSocketVersion.V13, null, true, httpHeaders);
        //客户端与服务端连接的通道，final修饰表示只会有一个
        final Channel channel = boot.connect(webSocketURI.getHost(), webSocketURI.getPort()).sync().channel();
        WebSocketClientHandler handler = (WebSocketClientHandler) channel.pipeline().get("hookedHandler");
        handler.setHandShaker(handsShaker);
        handsShaker.handshake(channel);
        //阻塞等待是否握手成功
        handler.handshakeFuture().sync();
    }

    public void close(){
        group.shutdownGracefully();
    }

}
