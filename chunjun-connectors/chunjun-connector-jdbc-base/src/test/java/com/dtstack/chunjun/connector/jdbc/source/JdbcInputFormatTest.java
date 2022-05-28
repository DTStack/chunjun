/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.jdbc.source;

import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

/** @author liuliu 2022/4/15 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JdbcInputFormat.class})
public class JdbcInputFormatTest {

    JdbcInputFormat jdbcInputFormat;

    @Before
    public void setup() {
        jdbcInputFormat = PowerMockito.mock(JdbcInputFormat.class);
        Logger LOG = PowerMockito.mock(Logger.class);
        Whitebox.setInternalState(jdbcInputFormat, "LOG", LOG);
        JdbcConf jdbcConf = PowerMockito.mock(JdbcConf.class);
        Whitebox.setInternalState(jdbcInputFormat, "jdbcConf", jdbcConf);
        PowerMockito.when(jdbcConf.getStartLocation()).thenReturn("10");
    }

    @Test
    public void createSplitsInternalBySplitRangeTest()
            throws InvocationTargetException, IllegalAccessException {
        PowerMockito.when(jdbcInputFormat.createSplitsInternalBySplitRange(Mockito.anyInt()))
                .thenCallRealMethod();
        Method getSplitRangeFromDb =
                PowerMockito.method(JdbcInputFormat.class, "getSplitRangeFromDb");
        Mockito.when(getSplitRangeFromDb.invoke(jdbcInputFormat))
                .thenReturn(Pair.of("12.123", "345534.12"));
        JdbcInputSplit[] splitsInternalBySplitRange =
                jdbcInputFormat.createSplitsInternalBySplitRange(3);
        Arrays.stream(splitsInternalBySplitRange).forEach(split -> System.out.println(split));
        assert splitsInternalBySplitRange.length == 3;
    }
}
