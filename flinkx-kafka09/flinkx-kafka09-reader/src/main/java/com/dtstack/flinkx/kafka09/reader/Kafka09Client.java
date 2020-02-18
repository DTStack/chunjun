/*
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

import com.dtstack.flinkx.kafkabase.decoder.IDecode;
import com.dtstack.flinkx.kafkabase.reader.IClient;
import com.dtstack.flinkx.kafkabase.reader.KafkaBaseInputFormat;
import com.dtstack.flinkx.util.ExceptionUtil;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Date: 2019/12/25
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class Kafka09Client implements IClient {

    private static final Logger LOG = LoggerFactory.getLogger(Kafka09Client.class);

    private volatile boolean running = true;
    private KafkaStream<byte[], byte[]> mStream;
    private IDecode decode;
    private KafkaBaseInputFormat format;

    public Kafka09Client(KafkaStream<byte[], byte[]> aStream, KafkaBaseInputFormat format) {
        this.mStream = aStream;
        this.decode = format.getDecode();
        this.format = format;
    }

    @Override
    public void run() {
        try {
            while (running) {
                ConsumerIterator<byte[], byte[]> it = mStream.iterator();
                while (it.hasNext()) {
                    String m = null;
                    try {
                        m = new String(it.next().message(), format.getEncoding());
                        Map<String, Object> event = this.decode.decode(m);
                        if (event != null && event.size() > 0) {
                            this.format.processEvent(event);
                        }
                    } catch (Exception e) {
                        LOG.error("process event = {}, e = {}", m, ExceptionUtil.getErrorMessage(e));
                    }
                }
            }
        } catch (Exception t) {
            LOG.error("kafka Consumer fetch error, e = {}", ExceptionUtil.getErrorMessage(t));
        }
    }

    @Override
    public void processMessage(String message) {
        Map<String, Object> event = decode.decode(message);
        if (event != null && event.size() > 0) {
            format.processEvent(event);
        }
    }

    @Override
    public void close() {
        running = false;
    }
}
