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

package com.dtstack.chunjun.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpeedConfigTest {

    @Test
    @DisplayName("Should set the rebalance to true")
    public void setRebalanceShouldSetTheRebalanceToTrue() {
        SpeedConfig speedConfig = new SpeedConfig();
        speedConfig.setRebalance(true);
        assertTrue(speedConfig.isRebalance());
    }

    @Test
    @DisplayName("Should set the rebalance to false")
    public void setRebalanceShouldSetTheRebalanceToFalse() {
        SpeedConfig speedConfig = new SpeedConfig();
        speedConfig.setRebalance(false);
        assertFalse(speedConfig.isRebalance());
    }

    @Test
    @DisplayName("Should set the bytes")
    public void setBytesShouldSetTheBytes() {
        SpeedConfig speedConfig = new SpeedConfig();
        long bytes = 100;

        speedConfig.setBytes(bytes);

        assertEquals(bytes, speedConfig.getBytes());
    }

    @Test
    @DisplayName("Should set the writerchannel to the given value")
    public void setWriterChannelShouldSetTheWriterChannelToTheGivenValue() {
        SpeedConfig speedConfig = new SpeedConfig();
        speedConfig.setWriterChannel(1);
        assertEquals(1, speedConfig.getWriterChannel());
    }

    @Test
    @DisplayName("Should set the readerchannel to the given value")
    public void setReaderChannelShouldSetTheReaderChannelToTheGivenValue() {
        SpeedConfig speedConfig = new SpeedConfig();
        speedConfig.setReaderChannel(1);
        assertEquals(1, speedConfig.getReaderChannel());
    }

    @Test
    @DisplayName("Should set the channel")
    public void setChannelShouldSetTheChannel() {
        SpeedConfig speedConfig = new SpeedConfig();
        speedConfig.setChannel(2);
        assertEquals(2, speedConfig.getChannel());
    }
}
