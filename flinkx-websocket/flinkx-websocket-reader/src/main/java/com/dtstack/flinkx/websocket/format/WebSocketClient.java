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

import com.dtstack.flinkx.util.ExceptionUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import static com.dtstack.flinkx.websocket.constants.WebSocketConfig.KEY_EXIT0;

/**
 * 基于webSocket的netty客户端
 * @Company: www.dtstack.com
 * @author kunni@dtstack.com
 */

public class WebSocketClient extends Thread{

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    private URI uri;

    private EventLoopGroup group;

    private WebSocketClientHandler webSocketClientHandler;

    /**
     * 重试次数
     */
    protected int retryTime = 5;

    /**
     * 重试间隔
     */
    protected int retryInterval = 5;

    protected SynchronousQueue<Row> queue;

    public WebSocketClient(SynchronousQueue<Row> queue, String serverUrl) throws URISyntaxException {
        uri = new URI(serverUrl);
        this.queue = queue;
    }

    @Override
    public void run() {
        if(retryTime==0){
            return;
        }
        Bootstrap boot = new Bootstrap();
        group = new NioEventLoopGroup();
        webSocketClientHandler = new WebSocketClientHandler(queue, uri, this);
        boot.option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .group(group)
                .handler(new LoggingHandler(LogLevel.INFO))
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) {
                        socketChannel.pipeline()
                        .addLast(new HttpClientCodec())
                        .addLast(new HttpObjectAggregator(1024*1024*10))
                        .addLast(webSocketClientHandler);
                    }
                });
        LOG.info("start bootstrapping");
        try{
            connect(boot, uri, retryTime, retryInterval);
        }catch (Exception e){
            LOG.error(ExceptionUtil.getErrorMessage(e));
        }
    }


    /**
     * 连接重试，等待时间随指数增长
     * @param boot 启动引导器
     * @param uri uri
     * @param retry 重试次数
     */
    public void connect(Bootstrap boot, URI uri, int retry, int delay){
        boot.connect(uri.getHost(), uri.getPort()).addListener((future)->{
            if(future.isSuccess()){
                LOG.info("connect success");
            }else if(retry == 0){
                LOG.error("connect failed");
                queue.put(Row.of(KEY_EXIT0));
                throw new RuntimeException("connect failed");
            }else{
                LOG.info("it's the {} time(s) try to connect to {}.", this.retryTime - retry + 1, uri.getRawPath());
                // 放到bootstrap重新调度运行
                boot.group().schedule(
                        () -> connect(boot, uri,retry-1, delay), delay, TimeUnit.SECONDS);
            }
        });
    }


    public WebSocketClient setRetryTime(int retryTime){
        this.retryTime = retryTime;
        return this;
    }

    public WebSocketClient setRetryInterval(int retryInterval){
        this.retryInterval = retryInterval;
        return this;
    }

    public void close(){
        group.shutdownGracefully();
    }

}
