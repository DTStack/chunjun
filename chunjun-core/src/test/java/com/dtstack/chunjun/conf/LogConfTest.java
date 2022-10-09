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

package com.dtstack.chunjun.conf;

import com.dtstack.chunjun.constants.ConfigConstant;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LogConfTest {

    /** Should return a string with all the fields */
    @Test
    public void toStringShouldReturnAStringWithAllTheFields() {
        LogConf logConf = new LogConf();
        logConf.setLogger(true);
        logConf.setLevel("info");
        logConf.setPath("/tmp/dtstack/chunjun/");
        logConf.setPattern(ConfigConstant.DEFAULT_LOG4J_PATTERN);

        assertEquals(
                "LogConf{isLogger=true, level='info', path='/tmp/dtstack/chunjun/', pattern='%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n'}",
                logConf.toString());
    }

    /** Should return the pattern when the pattern is not null */
    @Test
    public void getPatternWhenPatternIsNotNull() {
        LogConf logConf = new LogConf();
        logConf.setPattern("pattern");
        assertEquals("pattern", logConf.getPattern());
    }

    /** Should return the level when the level is not null */
    @Test
    public void getLevelWhenLevelIsNotNull() {
        LogConf logConf = new LogConf();
        logConf.setLevel("info");
        assertEquals("info", logConf.getLevel());
    }

    /** Should return the path */
    @Test
    public void getPathShouldReturnThePath() {
        LogConf logConf = new LogConf();
        logConf.setPath("/tmp/dtstack/chunjun/");
        assertEquals("/tmp/dtstack/chunjun/", logConf.getPath());
    }

    /** Should return true when the logger is set to true */
    @Test
    public void isLoggerWhenLoggerIsTrue() {
        LogConf logConf = new LogConf();
        logConf.setLogger(true);
        assertTrue(logConf.isLogger());
    }

    /** Should return false when the logger is set to false */
    @Test
    public void isLoggerWhenLoggerIsFalse() {
        LogConf logConf = new LogConf();
        assertFalse(logConf.isLogger());
    }
}
