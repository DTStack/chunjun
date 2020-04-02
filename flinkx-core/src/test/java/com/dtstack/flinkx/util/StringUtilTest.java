package com.dtstack.flinkx.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;

/**
 * @author jiangbo
 * @date 2020/3/12
 */
public class StringUtilTest {

    @Test
    public void testConvertRegularExpr() {
        String result = StringUtil.convertRegularExpr("\\t\\r\\n");
        Assert.assertEquals(result, "\t\r\n");

        result = StringUtil.convertRegularExpr(null);
        Assert.assertEquals(result, "");
    }

    @Test
    public void testString2col() {
        Object result = StringUtil.string2col("", "string", null);
        Assert.assertEquals(result, "");

        result = StringUtil.string2col("123", "TINYINT", null);
        Assert.assertEquals(result, Byte.valueOf("123"));

        result = StringUtil.string2col("1", "SMALLINT", null);
        Assert.assertEquals(result, (short)1);

        result = StringUtil.string2col("1", "INT", null);
        Assert.assertEquals(result, 1);

        result = StringUtil.string2col("1", "MEDIUMINT", null);
        Assert.assertEquals(result, (long)1);

        result = StringUtil.string2col("1", "BIGINT", null);
        Assert.assertEquals(result, (long)1);

        result = StringUtil.string2col("1.1", "float", null);
        Assert.assertEquals(result, (float)1.1);

        result = StringUtil.string2col("1.1", "double", null);
        Assert.assertEquals(result, 1.1);

        result = StringUtil.string2col("value", "string", null);
        Assert.assertEquals(result, "value");

        result = StringUtil.string2col("value", "string", null);
        Assert.assertEquals(result, "value");

        result = StringUtil.string2col("20200312-174412", "string", new SimpleDateFormat("yyyyMMdd-HHmmss"));
        Assert.assertEquals(result, "2020-03-12 17:44:12");

        result = StringUtil.string2col("true", "boolean", null);
        Assert.assertEquals(result, true);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HHmmss");
        java.sql.Date date = new java.sql.Date(System.currentTimeMillis());
        result = StringUtil.string2col(sdf.format(date), "date", sdf);
        Assert.assertEquals(result.toString(), date.toString());

        result = StringUtil.string2col("xxx", "xxx", null);
        Assert.assertEquals(result, "xxx");
    }
}