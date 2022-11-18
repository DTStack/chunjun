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

package com.dtstack.chunjun.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class SnowflakeIdWorkerTest {

    SnowflakeIdWorker snowflakeIdWorker = new SnowflakeIdWorker(1L, 1L);

    @Test
    public void testNextId() {
        Set<Long> idSet = new HashSet<>();
        int i = 0;
        while (i++ < 100) {
            long result = snowflakeIdWorker.nextId();
            idSet.add(result);
        }

        Assert.assertEquals(idSet.size(), 100);
    }
}
