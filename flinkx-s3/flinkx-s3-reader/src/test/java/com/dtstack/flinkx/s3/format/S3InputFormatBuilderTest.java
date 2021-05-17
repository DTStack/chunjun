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

package com.dtstack.flinkx.s3.format;

import com.dtstack.flinkx.s3.S3Config;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class S3InputFormatBuilderTest {

    private S3InputFormatBuilder s3InputFormatBuilder;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup(){
        s3InputFormatBuilder = new S3InputFormatBuilder();
    }

    @Test
    public void testCheckFormat(){
        S3Config s3Config = mock(S3Config.class);
        when(s3Config.getObjects()).thenReturn(null);
        when(s3Config.getAccessKey()).thenReturn("");
        when(s3Config.getSecretKey()).thenReturn("");
        when(s3Config.getBucket()).thenReturn("");
        s3InputFormatBuilder.setS3Config(s3Config);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("bucket was not supplied separately;\n" +
                "accessKey was not supplied separately;\n" +
                "accessKey was not supplied separately;\n" +
                "object was not supplied separately;");
        s3InputFormatBuilder.checkFormat();

        when(s3Config.getBucket()).thenReturn("test");
        s3InputFormatBuilder.checkFormat();
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("accessKey was not supplied separately;\n" +
                "accessKey was not supplied separately;\n" +
                "object was not supplied separately;");

        when(s3Config.getObjects()).thenReturn(new ArrayList<>());
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("accessKey was not supplied separately;\n" +
                "accessKey was not supplied separately;\n" +
                "object was not supplied separately;");
    }


}
