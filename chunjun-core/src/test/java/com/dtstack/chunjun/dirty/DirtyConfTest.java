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

package com.dtstack.chunjun.dirty;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DirtyConfTest {

    private DirtyConf dirtyConf;

    @BeforeEach
    public void setUp() {
        dirtyConf = new DirtyConf();
    }

    @Test
    @DisplayName("Should return a string with all the fields")
    public void toStringShouldReturnAStringWithAllTheFields() {
        dirtyConf.setMaxConsumed(1);
        dirtyConf.setMaxFailedConsumed(2);
        dirtyConf.setType("type");
        dirtyConf.setPrintRate(3L);
        dirtyConf.setPluginProperties(new Properties());
        dirtyConf.setLocalPluginPath("localPluginPath");

        String expected =
                "DirtyConf[maxConsumed=1, maxFailedConsumed=2, type='type', printRate=3, pluginProperties={}, localPluginPath='localPluginPath']";

        assertEquals(expected, dirtyConf.toString());
    }

    @Test
    @DisplayName("Should return the plugin properties")
    public void getPluginPropertiesShouldReturnThePluginProperties() {
        Properties pluginProperties = new Properties();
        dirtyConf.setPluginProperties(pluginProperties);
        assertEquals(pluginProperties, dirtyConf.getPluginProperties());
    }

    @Test
    @DisplayName("Should return the local plugin path")
    public void getLocalPluginPathShouldReturnTheLocalPluginPath() {
        DirtyConf dirtyConf = new DirtyConf();
        dirtyConf.setLocalPluginPath("/tmp/chunjun");
        assertEquals("/tmp/chunjun", dirtyConf.getLocalPluginPath());
    }

    @Test
    @DisplayName("Should return 1 when printrate is null")
    public void getPrintRateWhenPrintRateIsNullThenReturn1() {
        DirtyConf dirtyConf = new DirtyConf();
        assertEquals(1, dirtyConf.getPrintRate());
    }

    @Test
    @DisplayName("Should return printrate when printrate is not null")
    public void getPrintRateWhenPrintRateIsNotNullThenReturnPrintRate() {
        DirtyConf dirtyConf = new DirtyConf();
        dirtyConf.setPrintRate(1L);
        assertEquals(1L, dirtyConf.getPrintRate());
    }
}
