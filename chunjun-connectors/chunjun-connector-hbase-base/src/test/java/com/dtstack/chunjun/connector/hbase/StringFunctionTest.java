package com.dtstack.chunjun.connector.hbase;

import org.junit.Assert;
import org.junit.Test;

public class StringFunctionTest {
    @Test
    public void testStringFunction() {
        Assert.assertEquals("test", new StringFunction().evaluate("test"));
    }
}
