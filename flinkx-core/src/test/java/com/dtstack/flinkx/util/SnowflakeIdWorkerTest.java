package com.dtstack.flinkx.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * @author jiangbo
 * @date 2020/3/18
 */
public class SnowflakeIdWorkerTest {

    SnowflakeIdWorker snowflakeIdWorker = new SnowflakeIdWorker(1L, 1L);

    @Test
    public void testNextId() {
        Set<Long> idSet = new HashSet<>();
        int i = 0;
        while (i++ < 100) {
            long result = snowflakeIdWorker.nextId();
            idSet.add(result);
        }

        Assert.assertEquals(idSet.size(), 100);
    }
}