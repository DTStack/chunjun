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
package com.dtstack.flinkx.kafkabase.client;

/**
 * Date: 2019/12/25
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public interface IClient extends Runnable {

    /**
     * 线程方法
     */
    @Override
    void run();

    /**
     * 处理消息的方法
     * @param message       待处理的消息
     * @param topic         kafka topic
     * @param partition     kafka分区
     * @param offset        kafka offset
     * @param timestamp     kafka timestamp
     */
    void processMessage(String message, String topic, Integer partition, Long offset, Long timestamp);

    /**
     * 关闭连接
     */
    void close();
}
