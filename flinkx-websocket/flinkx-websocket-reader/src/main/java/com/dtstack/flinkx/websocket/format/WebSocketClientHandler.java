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
import com.dtstack.flinkx.util.GsonUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketHandshakeException;
import io.netty.util.CharsetUtil;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.SynchronousQueue;

/**
 * webSocket消息处理
 * @Company: www.dtstack.com
 * @author kunni
 */

public class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    private WebSocketClientHandshaker handShaker;

    private ChannelPromise handshakeFuture;

    private SynchronousQueue<Row> queue;

    public WebSocketClientHandler(SynchronousQueue<Row> queue) {
        this.queue = queue;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        LOG.info("current connect state : " + this.handShaker.isHandshakeComplete());
        Channel ch = ctx.channel();
        FullHttpResponse response;
        //进行握手操作
        if (!this.handShaker.isHandshakeComplete()) {
            try {
                response = (FullHttpResponse)msg;
                //握手协议返回，设置结束握手
                this.handShaker.finishHandshake(ch, response);
                //设置成功
                this.handshakeFuture.setSuccess();
                LOG.info("message" + response.headers());
            } catch (WebSocketHandshakeException var7) {
                FullHttpResponse res = (FullHttpResponse)msg;
                String errorMsg = String.format("connect failed, status:%s,reason:%s", res.status(), res.content().toString(CharsetUtil.UTF_8));
                this.handshakeFuture.setFailure(new Exception(errorMsg));
            }
        } else if (msg instanceof FullHttpResponse) {
            response = (FullHttpResponse)msg;
            throw new IllegalStateException("Unexpected FullHttpResponse (getStatus=" + response.status() + ", content=" + response.content().toString(CharsetUtil.UTF_8) + ')');
        } else {
            //接收服务端的消息
            WebSocketFrame frame = (WebSocketFrame)msg;
            Row row = null;
            //文本信息
            if (frame instanceof TextWebSocketFrame) {
                TextWebSocketFrame textFrame = (TextWebSocketFrame)frame;
                row = Row.of(textFrame.text());
            }
            //二进制信息
            if (frame instanceof BinaryWebSocketFrame) {
                BinaryWebSocketFrame binFrame = (BinaryWebSocketFrame)frame;
                row = Row.of(binFrame.content().array());
            }
            //ping信息
            if (frame instanceof PongWebSocketFrame) {
                LOG.info("WebSocket Client received pong");
            }
            //关闭消息
            if (frame instanceof CloseWebSocketFrame) {
                LOG.info("receive close frame");
                ch.close();
            }
            if(row != null){
                LOG.debug("row = {}", GsonUtil.GSON.toJson(row));
                queue.put(row);
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        LOG.info("connect success");
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        LOG.info("connection is closed by server");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("connect exception ：{}", ExceptionUtil.getErrorMessage(cause));
        ctx.close();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.handshakeFuture = ctx.newPromise();
    }

    public void setHandShaker(WebSocketClientHandshaker handShaker) {
        this.handShaker = handShaker;
    }

    public ChannelFuture handshakeFuture() {
        return this.handshakeFuture;
    }

}