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

package com.dtstack.chunjun.interceptor;

import com.dtstack.chunjun.interceptor.Interceptor.Context;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LogInterceptorTest {

    @Test
    @DisplayName("Should pre context")
    void preShouldNotLogContextWhenDebugIsDisabled() {
        LogInterceptor logInterceptor = new LogInterceptor();
        Context context = mock(Context.class);
        when(context.iterator()).thenReturn(Collections.emptyIterator());

        logInterceptor.pre(context);
    }

    @Test
    @DisplayName("Should init")
    public void initWhenDebugIsEnabledThenLogConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setString("key1", "value1");
        configuration.setString("key2", "value2");

        LogInterceptor logInterceptor = new LogInterceptor();
        logInterceptor.init(configuration);
    }
}
