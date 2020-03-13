package com.dtstack.flinkx.util;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jiangbo
 * @date 2020/3/12
 */
public class Md5UtilTest {

    @Test
    public void testGetMd5() {
        String result = Md5Util.getMd5("123456");
        Assert.assertEquals(result, "e10adc3949ba59abbe56e057f20f883e");
    }
}