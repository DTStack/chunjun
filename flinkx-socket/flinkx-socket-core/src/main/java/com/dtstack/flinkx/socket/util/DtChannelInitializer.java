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

import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.ValueUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.FixedLengthFrameDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.SynchronousQueue;

import static com.dtstack.flinkx.socket.constants.SocketCons.DECODE_ADJUSTMENT;
import static com.dtstack.flinkx.socket.constants.SocketCons.DECODE_DELIMITER;
import static com.dtstack.flinkx.socket.constants.SocketCons.DECODE_FILED_LENGTH;
import static com.dtstack.flinkx.socket.constants.SocketCons.DECODE_LENGTH;
import static com.dtstack.flinkx.socket.constants.SocketCons.DECODE_MAX_LENGTH;
import static com.dtstack.flinkx.socket.constants.SocketCons.DECODE_OFFSET;
import static com.dtstack.flinkx.socket.constants.SocketCons.DECODE_STRIP;
import static com.dtstack.flinkx.socket.constants.SocketCons.DEFAULT_IDLE_STATE_HANDLER_THREAD;
import static com.dtstack.flinkx.socket.constants.SocketCons.PREFIX_PACKAGE;

/**
 * Date: 2019/09/20
 * Company: www.dtstack.com
 *
 * @author kunni
 */

public class DtChannelInitializer extends ChannelInitializer<SocketChannel> {

    protected String binaryArrayDecoder;

    protected String byteBufDecoder;

    protected Map<String, Object> properties;

    protected transient SynchronousQueue<Row> queue;

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    private final EventExecutorGroup idleExecutorGroup = new DefaultEventExecutorGroup(DEFAULT_IDLE_STATE_HANDLER_THREAD);

    public DtChannelInitializer() {
    }

    @Override
    protected void initChannel(SocketChannel channel) {
        ChannelPipeline pipeLine = channel.pipeline();
        String keepAliveHandler = "keep-alive-handler";
        pipeLine.addLast(idleExecutorGroup, keepAliveHandler, new IdleStateHandler(60*15, 5, 0));
        pipeLine.addLast(getByteToMessageDecoder());
        pipeLine.addLast(getBinaryArrayDecoder());
    }

    /**
     *
     * @return 获取编解码器
     */
    protected ByteToMessageDecoder getByteToMessageDecoder() {
        switch(EByteBufDecoder.valueOf(StringUtils.upperCase(byteBufDecoder))) {
            case FIXEDLENGTHFRAMEDECODER : {
                return new FixedLengthFrameDecoder(ValueUtil.getIntegerVal(properties.get(DECODE_LENGTH)));
            }
            case DELIMITERBASEDFTAMEDECODER:{
                return new DelimiterBasedFrameDecoder(ValueUtil.getIntegerVal(properties.get(DECODE_MAX_LENGTH), 1024), Unpooled.copiedBuffer(ValueUtil.getStringVal(properties.get(DECODE_DELIMITER)).getBytes()));
            }
            case LENGTHFIELDBASEDFRAMEDECODER:{
                int maxLength = ValueUtil.getIntegerVal(properties.get(DECODE_MAX_LENGTH), Integer.MAX_VALUE);
                int offset = ValueUtil.getIntegerVal(properties.get(DECODE_OFFSET));
                int fieldLength = ValueUtil.getIntegerVal(properties.get(DECODE_FILED_LENGTH));
                int adjustment = ValueUtil.getIntegerVal(properties.get(DECODE_ADJUSTMENT));
                int strip = ValueUtil.getIntegerVal(properties.get(DECODE_STRIP));
                return new LengthFieldBasedFrameDecoder(maxLength , offset, fieldLength, adjustment, strip);
            }
            default:{
                try{
                    Class<?> clz = Class.forName(PREFIX_PACKAGE + byteBufDecoder);
                    return (ByteToMessageDecoder) clz.getConstructor().newInstance();
                }catch (Exception e){
                    LOG.error("please check Class Name");
                    LOG.error(ExceptionUtil.getErrorMessage(e));
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public DtClientHandler getBinaryArrayDecoder() {
        return new DtClientHandler(queue, binaryArrayDecoder);
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public void setByteBufDecoder(String byteBufDecoder) {
        this.byteBufDecoder = byteBufDecoder;
    }

    public void setBinaryArrayDecoder(String binaryArrayDecoder) {
        this.binaryArrayDecoder = binaryArrayDecoder;
    }

    public void setQueue(SynchronousQueue<Row> queue) {
        this.queue = queue;
    }

}
