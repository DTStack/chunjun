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

package com.dtstack.chunjun.conf;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpeedConfTest {

    @Test
    @DisplayName("Should set the rebalance to true")
    public void setRebalanceShouldSetTheRebalanceToTrue() {
        SpeedConf speedConf = new SpeedConf();
        speedConf.setRebalance(true);
        assertTrue(speedConf.isRebalance());
    }

    @Test
    @DisplayName("Should set the rebalance to false")
    public void setRebalanceShouldSetTheRebalanceToFalse() {
        SpeedConf speedConf = new SpeedConf();
        speedConf.setRebalance(false);
        assertFalse(speedConf.isRebalance());
    }

    @Test
    @DisplayName("Should set the bytes")
    public void setBytesShouldSetTheBytes() {
        SpeedConf speedConf = new SpeedConf();
        long bytes = 100;

        speedConf.setBytes(bytes);

        assertEquals(bytes, speedConf.getBytes());
    }

    @Test
    @DisplayName("Should set the writerchannel to the given value")
    public void setWriterChannelShouldSetTheWriterChannelToTheGivenValue() {
        SpeedConf speedConf = new SpeedConf();
        speedConf.setWriterChannel(1);
        assertEquals(1, speedConf.getWriterChannel());
    }

    @Test
    @DisplayName("Should set the readerchannel to the given value")
    public void setReaderChannelShouldSetTheReaderChannelToTheGivenValue() {
        SpeedConf speedConf = new SpeedConf();
        speedConf.setReaderChannel(1);
        assertEquals(1, speedConf.getReaderChannel());
    }

    @Test
    @DisplayName("Should set the channel")
    public void setChannelShouldSetTheChannel() {
        SpeedConf speedConf = new SpeedConf();
        speedConf.setChannel(2);
        assertEquals(2, speedConf.getChannel());
    }
}
