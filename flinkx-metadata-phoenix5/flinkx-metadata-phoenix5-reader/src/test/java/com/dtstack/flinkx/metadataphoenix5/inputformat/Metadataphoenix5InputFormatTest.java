package com.dtstack.flinkx.metadataphoenix5.inputformat;

import org.junit.Assert;
import org.junit.Test;

public class Metadataphoenix5InputFormatTest {

    protected Metadataphoenix5InputFormat inputFormat = new Metadataphoenix5InputFormat();

    @Test
    public void testSetPath(){
        inputFormat.setPath("/hbase");
        Assert.assertEquals(inputFormat.path, "/hbase/table");
    }

    @Test
    public void testQuote(){
        Assert.assertEquals(inputFormat.quote("test"), "Test");
    }

}
