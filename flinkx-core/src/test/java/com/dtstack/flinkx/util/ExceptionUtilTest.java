package com.dtstack.flinkx.util;

import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jiangbo
 * @date 2020/3/18
 */
public class ExceptionUtilTest {

    @Test
    public void testGetErrorMessage() {
        String result = ExceptionUtil.getErrorMessage(null);
        Assert.assertNull(result);

        try {
            ExceptionUtil.getErrorMessage(new IllegalArgumentException("error test"));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}