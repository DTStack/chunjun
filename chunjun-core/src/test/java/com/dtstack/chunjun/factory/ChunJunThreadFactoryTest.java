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

package com.dtstack.chunjun.factory;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunJunThreadFactoryTest {

    @Test
    @DisplayName("Should create a thread with the given name")
    public void newThreadShouldCreateAThreadWithTheGivenName() {
        ChunJunThreadFactory chunJunThreadFactory = new ChunJunThreadFactory("test");
        Thread thread = chunJunThreadFactory.newThread(() -> {});
        assertTrue(thread.getName().startsWith("test"));
    }

    @Test
    @DisplayName("Should create a thread with the given name and daemon")
    public void newThreadShouldCreateAThreadWithTheGivenNameAndDaemon() {
        ChunJunThreadFactory chunJunThreadFactory = new ChunJunThreadFactory("test", true);
        Thread thread = chunJunThreadFactory.newThread(() -> {});
        assertTrue(thread.isDaemon());
        assertEquals("test-pool-1-thread-1", thread.getName());
    }
}
