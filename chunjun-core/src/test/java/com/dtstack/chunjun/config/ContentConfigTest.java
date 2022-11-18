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

import com.dtstack.chunjun.cdc.CdcConfig;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ContentConfigTest {

    /** Should return a string that contains all the fields of the class */
    @Test
    public void toStringShouldReturnAStringThatContainsAllTheFieldsOfTheClass() {
        ContentConfig contentConfig = new ContentConfig();
        contentConfig.setReader(new OperatorConfig());
        contentConfig.setWriter(new OperatorConfig());
        contentConfig.setTransformer(new TransformerConfig());
        contentConfig.setRestoration(new CdcConfig());

        String toString = contentConfig.toString();

        assertTrue(toString.contains("reader"));
        assertTrue(toString.contains("writer"));
        assertTrue(toString.contains("transformer"));
        assertTrue(toString.contains("restoration"));
    }

    /** Should return null when the nameMapping is null */
    @Test
    public void getNameMappingWhenNameMappingIsNull() {
        ContentConfig contentConfig = new ContentConfig();
        assertNull(contentConfig.getNameMapping());
    }

    /** Should return the restoration */
    @Test
    public void getRestorationShouldReturnTheRestoration() {
        ContentConfig contentConfig = new ContentConfig();
        CdcConfig cdcConfig = new CdcConfig();
        contentConfig.setRestoration(cdcConfig);
        assertEquals(cdcConfig, contentConfig.getRestoration());
    }

    /** Should return the transformer */
    @Test
    public void getTransformerShouldReturnTheTransformer() {
        ContentConfig contentConfig = new ContentConfig();
        TransformerConfig transformerConfig = new TransformerConfig();
        contentConfig.setTransformer(transformerConfig);
        assertEquals(transformerConfig, contentConfig.getTransformer());
    }

    /** Should return the writer */
    @Test
    public void getWriterShouldReturnTheWriter() {
        ContentConfig contentConfig = new ContentConfig();
        OperatorConfig operatorConfig = new OperatorConfig();
        contentConfig.setWriter(operatorConfig);
        assertEquals(operatorConfig, contentConfig.getWriter());
    }

    /** Should return the reader */
    @Test
    public void getReaderShouldReturnTheReader() {
        ContentConfig contentConfig = new ContentConfig();
        OperatorConfig reader = new OperatorConfig();
        contentConfig.setReader(reader);
        assertEquals(reader, contentConfig.getReader());
    }
}
