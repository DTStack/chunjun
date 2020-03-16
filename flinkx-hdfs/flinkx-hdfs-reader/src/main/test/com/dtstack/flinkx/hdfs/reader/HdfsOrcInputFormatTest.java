package com.dtstack.flinkx.hdfs.reader;

import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @author jiangbo
 * @date 2020/3/16
 */
public class HdfsOrcInputFormatTest {

    @InjectMocks
    HdfsOrcInputFormat hdfsOrcInputFormat;

    @BeforeClass
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testParseColumnAndType() {
        String struct = "int,float(10,2),char(12)";
        List<String> result = hdfsOrcInputFormat.parseColumnAndType(struct);
        Assert.assertEquals(result.size(), 3);
    }
}