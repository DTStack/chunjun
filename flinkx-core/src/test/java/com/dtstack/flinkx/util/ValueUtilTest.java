package com.dtstack.flinkx.util;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jiangbo
 * @date 2020/3/13
 */
public class ValueUtilTest {

    @Test(expectedExceptions = RuntimeException.class)
    public void testGetInt() {
        Integer result = ValueUtil.getInt(null);
        Assert.assertNull(result);

        result = ValueUtil.getInt("100");
        Assert.assertEquals(result, new Integer(100));

        result = ValueUtil.getInt(new Long(100));
        Assert.assertEquals(result, new Integer(100));

        ValueUtil.getInt(new Object());
    }
}