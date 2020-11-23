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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketHandshakeException;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.util.CharsetUtil;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.SynchronousQueue;

/**
 * webSocket消息处理
 * @Company: www.dtstack.com
 * @author kunni@dtstack.com
 */

@ChannelHandler.Sharable
public class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    private WebSocketClientHandshaker handShaker;

    /**
     * 存放握手是否成功的Promise
     */
    private ChannelPromise handshakeFuture;

    private SynchronousQueue<Row> queue;

    /**
     * 保存client，用于重连
     */
    private WebSocketClient client;

    protected String message;

    public WebSocketClientHandler(SynchronousQueue<Row> queue, URI uri, WebSocketClient client) {
        this.queue = queue;
        this.client = client;
        // 采用默认生成
        handShaker = WebSocketClientHandshakerFactory.newHandshaker(
                uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders());
    }
    /**
     * 创建一个新的Promise
     * 成功或失败在{@link WebSocketClientHandler#channelRead0(ChannelHandlerContext, Object)}设置
     */
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.handshakeFuture = ctx.newPromise();
        handshakeFuture.addListener((future)-> {
            if(future.isSuccess()){
                LOG.info("handshake success!");
                // 发送开启读写信息
                WebSocketFrame frame = new TextWebSocketFrame(message);
                ctx.channel().writeAndFlush(frame);
            }else {
                LOG.info("handShake failed");
            }
        });
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        handShaker.handshake(ctx.channel());
    }

    /**
     * 关闭channel，设置异常失败标志
     * @param ctx channel上下文
     * @param cause 异常
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("connect exception ：{}", ExceptionUtil.getErrorMessage(cause));
        if (!handshakeFuture.isDone()) {
            handshakeFuture.setFailure(cause);
        }
        ctx.close();
        LOG.info("connection is closed by server");
        LOG.info("reconnecting .......");
        // 通过调用run方法，实现重连尝试
        client.run();
    }

    /**
     * 处理读取事件
     * @param ctx channel上下文
     * @param msg 消息体
     * @throws Exception 异常
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        Channel ch = ctx.channel();
        // 设置握手结果
        if (!handShaker.isHandshakeComplete()) {
            try {
                handShaker.finishHandshake(ch, (FullHttpResponse) msg);
                LOG.info("WebSocket Client connected!");
                handshakeFuture.setSuccess();
            } catch (WebSocketHandshakeException e) {
                LOG.info("WebSocket Client failed to connect");
                handshakeFuture.setFailure(e);
            }
            return;
        }
        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;
            throw new IllegalStateException("Unexpected FullHttpResponse (content=" + response.content().toString(CharsetUtil.UTF_8) + ')');
        }
        //接收服务端的消息
        WebSocketFrame frame = (WebSocketFrame)msg;
        //文本信息
        if (frame instanceof TextWebSocketFrame) {
            TextWebSocketFrame textFrame = (TextWebSocketFrame)frame;
            queue.put(Row.of(textFrame.text()));
            LOG.debug("print webSocket message: {}", textFrame.text());
        }
        //ping信息
        if (frame instanceof PongWebSocketFrame) {
            LOG.info("WebSocket Client received pong");
        }
        //关闭消息
        if (frame instanceof CloseWebSocketFrame) {
            LOG.info("receive close frame");
            ch.close();
            // 进行重连尝试以设置任务运行结果
            client.run();
        }
    }

    public void setMessage(String message){
        this.message = message;
    }

}