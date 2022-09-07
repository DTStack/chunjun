package com.dtstack.chunjun.connector.hbase;

import com.dtstack.chunjun.util.Md5Util;

import org.junit.Assert;
import org.junit.Test;

public class Md5FunctionTest {

    @Test
    public void testMd5() throws Exception {
        Assert.assertEquals(Md5Util.getMd5("test"), new Md5Function().evaluate("test"));
    }
}
