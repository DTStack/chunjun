package com.dtstack.flinkx.util;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jiangbo
 * @date 2020/3/12
 */
public class ColumnTypeUtilTest {

    @Test
    public void testIsDecimalType() {
        boolean result = ColumnTypeUtil.isDecimalType("decimal");
        Assert.assertTrue(result);
    }

    @Test
    public void testGetDecimalInfo() {
        ColumnTypeUtil.DecimalInfo defaultDecimal = new ColumnTypeUtil.DecimalInfo(10, 2);

        ColumnTypeUtil.DecimalInfo result = ColumnTypeUtil.getDecimalInfo("decimal", defaultDecimal);
        Assert.assertEquals(result, defaultDecimal);

        result = ColumnTypeUtil.getDecimalInfo("decimal(10,2)", null);
        Assert.assertEquals(result, defaultDecimal);

        try {
            ColumnTypeUtil.getDecimalInfo("int", null);
        } catch (Throwable e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
    }
}
