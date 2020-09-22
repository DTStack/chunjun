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

import com.dtstack.flinkx.decoder.DecodeEnum;
import com.dtstack.flinkx.decoder.IDecode;
import com.dtstack.flinkx.decoder.JsonDecoder;
import com.dtstack.flinkx.decoder.PlainDecoder;
import com.dtstack.flinkx.util.ExceptionUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.SynchronousQueue;

import static com.dtstack.flinkx.socket.constants.SocketCons.PREFIX_PACKAGE;

/** 自定义handler
 * Date: 2019/09/20
 * Company: www.dtstack.com
 * @author kunni@dtstack.com
 */

public class DtClientHandler extends ChannelInboundHandlerAdapter {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected transient SynchronousQueue<Row> queue;

    protected IDecode decoder;

    public DtClientHandler(SynchronousQueue<Row> queue, String binaryArrayDecoder){
        this.queue = queue;
        decoder = getDecoder(binaryArrayDecoder);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Map<String, Object> event = decoder.decode((String) msg);
        Row row = new Row(event.size());
        int count = 0;
        for(Map.Entry<String, Object> entry : event.entrySet()){
            row.setField(count, entry.getValue());
        }
        try{
            queue.put(row);
        }catch (InterruptedException e){
            LOG.error(ExceptionUtil.getErrorMessage(e));
        }
    }

    public IDecode getDecoder(String binaryArrayDecoder){
        switch (DecodeEnum.valueOf(StringUtils.upperCase(binaryArrayDecoder))){
            case JSON:{
                return new JsonDecoder();
            }
            case PLAIN:{
                return new PlainDecoder();
            }
            default:{
                try{
                    Class<?> clz = Class.forName(PREFIX_PACKAGE + binaryArrayDecoder);
                    return (IDecode) clz.getConstructor().newInstance();
                }catch (Exception e){
                    LOG.error("please check Class Name");
                    LOG.error(ExceptionUtil.getErrorMessage(e));
                    throw new RuntimeException(e);
                }

            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error(ExceptionUtil.getErrorMessage(cause));
        ctx.close();
    }
}
