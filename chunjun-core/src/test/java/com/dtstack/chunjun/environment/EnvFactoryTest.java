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

package com.dtstack.chunjun.environment;

import com.dtstack.chunjun.options.Options;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EnvFactoryTest {
    @Test
    @DisplayName("Should set table config for python when the key starts with python.")
    public void createStreamTableEnvironmentShouldSetTableConfigForPythonWhenKeyStartsWithPython() {
        StreamExecutionEnvironment env = mock(StreamExecutionEnvironment.class);
        StreamTableEnvironment tEnv = mock(StreamTableEnvironment.class);
        TableConfig tableConfig = mock(TableConfig.class);
        Configuration configuration = mock(Configuration.class);
        Properties properties = new Properties();
        properties.setProperty("python.key", "value");

        when(tEnv.getConfig()).thenReturn(tableConfig);
        when(tableConfig.getConfiguration()).thenReturn(configuration);

        EnvFactory.createStreamTableEnvironment(env, properties, "jobName");
    }

    @Test
    @DisplayName("Should return a streamexecutionenvironment when the mode is local")
    public void createStreamExecutionEnvironmentWhenModeIsLocal() {
        Options options = new Options();
        options.setMode("local");
        StreamExecutionEnvironment env = EnvFactory.createStreamExecutionEnvironment(options);
        assertTrue(env instanceof MyLocalStreamEnvironment);
    }

    @Test
    @DisplayName("Should return a streamexecutionenvironment when the mode is not local")
    public void createStreamExecutionEnvironmentWhenModeIsNotLocal() {
        Options options = new Options();
        options.setMode("standalone");
        StreamExecutionEnvironment env = EnvFactory.createStreamExecutionEnvironment(options);
        assertTrue(env instanceof StreamExecutionEnvironment);
    }
}
