package com.dtstack.flinkx.util;

import org.apache.flink.types.Row;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jiangbo
 * @date 2020/3/13
 */
public class RowUtilTest {

    @Test
    public void testRowToJson() {
        Row row = new Row(2);
        row.setField(0, 1);
        row.setField(1, "val");
        String result = RowUtil.rowToJson(row, new String[]{"col1", "col2"});
        Assert.assertEquals(result, "{\"col1\":1,\"col2\":\"val\"}");
    }
}