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