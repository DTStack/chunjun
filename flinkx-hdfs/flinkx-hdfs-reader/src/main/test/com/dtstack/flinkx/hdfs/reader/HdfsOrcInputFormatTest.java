package com.dtstack.flinkx.hdfs.reader;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * @author jiangbo
 * @date 2020/3/16
 */
public class HdfsOrcInputFormatTest {

    @Test
    public void testParseColumnAndType() {
        HdfsOrcInputFormat hdfsOrcInputFormat = new HdfsOrcInputFormat();

        String struct = "int,float(10,2),char(12)";
        List<String> result = hdfsOrcInputFormat.parseColumnAndType(struct);
        Assert.assertEquals(result.size(), 3);
    }
}