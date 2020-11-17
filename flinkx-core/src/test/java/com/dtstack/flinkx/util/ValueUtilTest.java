package com.dtstack.flinkx.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author jiangbo
 * @date 2020/3/13
 */
public class ValueUtilTest {

    @Test
    public void testGetInt() {
        Integer result = ValueUtil.getInt(null);
        Assert.assertNull(result);

        result = ValueUtil.getInt("100");
        Assert.assertEquals(result, new Integer(100));

        result = ValueUtil.getInt(new Long(100));
        Assert.assertEquals(result, new Integer(100));

        try {
            ValueUtil.getInt(new Object());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Unable to convert"));
        }
    }
}