/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.rocketmq.utils;

import com.dtstack.chunjun.connector.rocketmq.config.RocketMQConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public final class RocketMQUtils {

    public static String getInstanceName(String... args) {
        if (null != args && args.length > 0) {
            return String.join("_", args);
        }
        return ManagementFactory.getRuntimeMXBean().getName() + "_" + System.nanoTime();
    }

    /**
     * Average Hashing queue algorithm Refer:
     * org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely
     */
    public static List<MessageQueue> allocate(
            Collection<MessageQueue> mqSet, int numberOfParallelTasks, int indexOfThisTask) {
        ArrayList<MessageQueue> mqAll = new ArrayList<>(mqSet);
        Collections.sort(mqAll);
        List<MessageQueue> result = new ArrayList<>();
        int mod = mqAll.size() % numberOfParallelTasks;
        int averageSize =
                mqAll.size() <= numberOfParallelTasks
                        ? 1
                        : (mod > 0 && indexOfThisTask < mod
                                ? mqAll.size() / numberOfParallelTasks + 1
                                : mqAll.size() / numberOfParallelTasks);
        int startIndex =
                (mod > 0 && indexOfThisTask < mod)
                        ? indexOfThisTask * averageSize
                        : indexOfThisTask * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    public static AclClientRPCHook buildAclRPCHook(String accessKey, String secretKey) {
        if (!StringUtils.isEmpty(accessKey) && !StringUtils.isEmpty(secretKey)) {
            return new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
        }
        return null;
    }

    public static void buildConsumer(RocketMQConfig conf, DefaultMQPullConsumer consumer) {
        Validate.notEmpty(conf.getNameserverAddress());
        consumer.setNamesrvAddr(conf.getNameserverAddress());
        consumer.setHeartbeatBrokerInterval(conf.getHeartbeatBrokerInterval());
        // When using aliyun products, you need to set up channels
        consumer.setAccessChannel(AccessChannel.valueOf(conf.getAccessChannel()));
        consumer.setUnitName(conf.getUnitName());

        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setPersistConsumerOffsetInterval(conf.getPersistConsumerOffsetInterval());
    }
}
