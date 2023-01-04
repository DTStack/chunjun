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

package com.dtstack.chunjun.dirty.utils;

import com.dtstack.chunjun.dirty.DirtyConfig;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DirtyConfigUtilTest {
    @Test
    @DisplayName("Should return dirtyconf when the type is default")
    public void parseWhenTypeIsDefault() {
        Map<String, String> confMap = new HashMap<>();
        confMap.put(DirtyConfUtil.TYPE_KEY, "default");
        confMap.put(DirtyConfUtil.MAX_ROWS_KEY, "100");
        confMap.put(DirtyConfUtil.MAX_FAILED_ROWS_KEY, "200");
        confMap.put(DirtyConfUtil.PRINT_INTERVAL, "300");
        confMap.put(DirtyConfUtil.DIRTY_DIR, "/tmp/dirty");

        DirtyConfig dirtyConfig = DirtyConfUtil.parseFromMap(confMap);

        assertEquals("default", dirtyConfig.getType());
        assertEquals(100, dirtyConfig.getMaxConsumed());
        assertEquals(200, dirtyConfig.getMaxFailedConsumed());
        assertEquals(300, dirtyConfig.getPrintRate());
        assertEquals("/tmp/dirty", dirtyConfig.getLocalPluginPath());
    }

    @Test
    @DisplayName("Should return dirtyconf when the type is jdbc")
    public void parseWhenTypeIsJdbc() {
        Map<String, String> confMap = new HashMap<>();
        confMap.put(DirtyConfUtil.TYPE_KEY, "jdbc");
        confMap.put(DirtyConfUtil.MAX_ROWS_KEY, "100");
        confMap.put(DirtyConfUtil.MAX_FAILED_ROWS_KEY, "200");
        confMap.put(DirtyConfUtil.PRINT_INTERVAL, "300");
        confMap.put(DirtyConfUtil.DIRTY_DIR, "/tmp/dirty");

        DirtyConfig dirtyConfig = DirtyConfUtil.parseFromMap(confMap);

        assertEquals("mysql", dirtyConfig.getType());
        assertEquals(100, dirtyConfig.getMaxConsumed());
        assertEquals(200, dirtyConfig.getMaxFailedConsumed());
        assertEquals(300, dirtyConfig.getPrintRate());
        assertEquals("/tmp/dirty", dirtyConfig.getLocalPluginPath());
    }
}
