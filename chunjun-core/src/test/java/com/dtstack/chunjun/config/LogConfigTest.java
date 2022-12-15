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

package com.dtstack.chunjun.config;

import com.dtstack.chunjun.constants.ConfigConstant;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LogConfigTest {

    /** Should return a string with all the fields */
    @Test
    public void toStringShouldReturnAStringWithAllTheFields() {
        LogConfig logConfig = new LogConfig();
        logConfig.setLogger(true);
        logConfig.setLevel("info");
        logConfig.setPath("/tmp/dtstack/chunjun/");
        logConfig.setPattern(ConfigConstant.DEFAULT_LOG4J_PATTERN);

        assertEquals(
                "LogConfig[isLogger=true, level='info', path='/tmp/dtstack/chunjun/', pattern='%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n']",
                logConfig.toString());
    }

    /** Should return the pattern when the pattern is not null */
    @Test
    public void getPatternWhenPatternIsNotNull() {
        LogConfig logConfig = new LogConfig();
        logConfig.setPattern("pattern");
        assertEquals("pattern", logConfig.getPattern());
    }

    /** Should return the level when the level is not null */
    @Test
    public void getLevelWhenLevelIsNotNull() {
        LogConfig logConfig = new LogConfig();
        logConfig.setLevel("info");
        assertEquals("info", logConfig.getLevel());
    }

    /** Should return the path */
    @Test
    public void getPathShouldReturnThePath() {
        LogConfig logConfig = new LogConfig();
        logConfig.setPath("/tmp/dtstack/chunjun/");
        assertEquals("/tmp/dtstack/chunjun/", logConfig.getPath());
    }

    /** Should return true when the logger is set to true */
    @Test
    public void isLoggerWhenLoggerIsTrue() {
        LogConfig logConfig = new LogConfig();
        logConfig.setLogger(true);
        assertTrue(logConfig.isLogger());
    }

    /** Should return false when the logger is set to false */
    @Test
    public void isLoggerWhenLoggerIsFalse() {
        LogConfig logConfig = new LogConfig();
        assertFalse(logConfig.isLogger());
    }
}
