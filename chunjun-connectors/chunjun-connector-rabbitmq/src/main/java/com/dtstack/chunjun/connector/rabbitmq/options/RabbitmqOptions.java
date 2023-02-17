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

package com.dtstack.chunjun.connector.rabbitmq.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class RabbitmqOptions {
    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("RabbitMQ host");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(5672)
                    .withDescription("RabbitMQ service port");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("RabbitMQ username");
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("RabbitMQ password");

    public static final ConfigOption<String> VIRTUAL_HOST =
            ConfigOptions.key("virtual-host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("RabbitMq virtual host of the username");

    public static final ConfigOption<Integer> NETWORK_RECOVERY_INTERVAL =
            ConfigOptions.key("network-recovery-interval")
                    .intType()
                    .noDefaultValue()
                    .withDescription("RabbitMq connection recovery interval in milliseconds");
    public static final ConfigOption<Boolean> AUTOMATIC_RECOVERY =
            ConfigOptions.key("automatic-recovery")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("RabbitMq if automatic connection recovery");
    public static final ConfigOption<Boolean> TOPOLOGY_RECOVERY =
            ConfigOptions.key("topology-recovery")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("RabbitMq if topology recovery");
    public static final ConfigOption<Integer> CONNECTION_TIMEOUT =
            ConfigOptions.key("connection-timeout")
                    .intType()
                    .noDefaultValue()
                    .withDescription("RabbitMq connection timeout time");
    public static final ConfigOption<Integer> REQUESTED_CHANNEL_MAX =
            ConfigOptions.key("requested-channel-max")
                    .intType()
                    .noDefaultValue()
                    .withDescription("RabbitMq requested maximum channel number");
    public static final ConfigOption<Integer> REQUESTED_FRAME_MAX =
            ConfigOptions.key("requested-frame-max")
                    .intType()
                    .noDefaultValue()
                    .withDescription("RabbitMq requested maximum frame size");
    public static final ConfigOption<Integer> REQUESTED_HEARTBEAT =
            ConfigOptions.key("requested-heartbeat")
                    .intType()
                    .noDefaultValue()
                    .withDescription("RabbitMq requested heartbeat interval");
    public static final ConfigOption<Long> DELIVERY_TIMEOUT =
            ConfigOptions.key("delivery-timeout")
                    .longType()
                    .noDefaultValue()
                    .withDescription("RabbitMq message delivery timeout in the queueing consumer");
    public static final ConfigOption<Integer> PREFETCH_COUNT =
            ConfigOptions.key("prefetch-count")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "RabbitMq maximum number of messages that the server will deliver, 0 if unlimited, the prefetch count must be between 0 and 65535 (unsigned short in AMQP 0-9-1)");

    public static final ConfigOption<Boolean> USE_CORRELATION_ID =
            ConfigOptions.key("use-correlationId")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether the messages received are supplied with a unique id to deduplicate messages (in case of failed acknowledgments). Only used when checkpointing is enabled.");
    public static final ConfigOption<String> QUEUE_NAME =
            ConfigOptions.key("queue-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The queue to receive messages from.");
}
