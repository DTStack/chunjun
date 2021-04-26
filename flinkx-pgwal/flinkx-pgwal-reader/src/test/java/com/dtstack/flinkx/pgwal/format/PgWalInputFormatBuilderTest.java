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
package com.dtstack.flinkx.pgwal.format;

import com.google.common.collect.Lists;
import junit.framework.TestCase;
import org.junit.Assert;

public class PgWalInputFormatBuilderTest extends TestCase {

    private PgWalInputFormatBuilder builder;
    public void setUp() throws Exception {
        super.setUp();
         builder = new PgWalInputFormatBuilder();
    }

    public void tearDown() throws Exception {
        builder = null;
    }

    public void testSetUsername() {
        builder.setUsername("a");
        Assert.assertEquals("a", builder.getUsername());
    }

    public void testSetPassword() {
        builder.setPassword("a");
        Assert.assertEquals("a", builder.getPassword());
    }

    public void testSetUrl() {
        builder.setUrl("a");
        Assert.assertEquals("a", builder.getUrl());
    }

    public void testSetDatabaseName() {
        builder.setDatabaseName("a");
        Assert.assertEquals("a", builder.getDatabaseName());
    }

    public void testSetPavingData() {
        builder.setPavingData(true);
        Assert.assertTrue(builder.isPavingData());
    }

    public void testSetTableList() {
        builder.setTableList(Lists.newArrayList("a"));
        Assert.assertEquals(Lists.newArrayList("a"), builder.getTableList());
    }

    public void testSetCat() {
        builder.setCat("a");
        assertEquals("a", builder.getCat());
    }

    public void testSetStatusInterval() {
        builder.setStatusInterval(1);
        assertEquals(1, builder.getStatusInterval());
    }

    public void testSetLsn() {
        builder.setLsn(1L);
        assertEquals(1L, builder.getLsn());
    }

    public void testSetAllowCreateSlot() {
        builder.setAllowCreateSlot(true);
        assertTrue(builder.isAllowCreateSlot());
    }

    public void testSetSlotName() {
        builder.setSlotName("a");
        assertEquals("a", builder.getSlotName());
    }

    public void testSetTemporary() {
        builder.setTemporary(true);
        assertTrue(builder.isTemporary());
    }

    public void testSetPublicationName() {
        builder.setPublicationName("a");
        assertEquals("a", builder.getPublicationName());
    }

    public void testSetConnectionTimeoutSecond() {
        builder.setConnectionTimeoutSecond(1);
        assertEquals(1, builder.getConnectionTimeoutSecond());
    }

    public void testSetSocketTimeoutSecond() {
        builder.setSocketTimeoutSecond(1);
        assertEquals(1, builder.getSocketTimeoutSecond());
    }

    public void testSetLoginTimeoutSecond() {
        builder.setLoginTimeoutSecond(1);
        assertEquals(1, builder.getLoginTimeoutSecond());
    }
}