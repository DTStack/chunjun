/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.kafka09.reader;

import com.dtstack.flinkx.kafka09.decoder.IDecode;
import com.dtstack.flinkx.kafka09.decoder.JsonDecoder;
import com.dtstack.flinkx.kafka09.decoder.PlainDecoder;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/7/5
 */
public class KafkaConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

    private KafkaStream<byte[], byte[]> m_stream;
    private Kafka09InputFormat format;
    private IDecode decoder;

    public KafkaConsumer(KafkaStream<byte[], byte[]> a_stream, Kafka09InputFormat format) {
        this.m_stream = a_stream;
        this.format = format;
        this.decoder = createDecoder();
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
                while (it.hasNext()) {
                    String m = null;
                    try {
                        m = new String(it.next().message(), format.getEncoding());
                        Map<String, Object> event = this.decoder.decode(m);
                        if (event != null && event.size() > 0) {
                            this.format.processEvent(event);
                        }
                    } catch (Exception e) {
                        LOG.error("process event:{} failed:{}", m, e.getCause());
                    }
                }
            }
        } catch (Exception t) {
            LOG.error("kakfa Consumer fetch is error:{}", t.getCause());
        }
    }

    private IDecode createDecoder() {
        if ("json".equals(format.getCodec())) {
            return new JsonDecoder();
        } else {
            return new PlainDecoder();
        }
    }
}
