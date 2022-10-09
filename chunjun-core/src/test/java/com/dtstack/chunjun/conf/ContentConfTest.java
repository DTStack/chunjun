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

import com.dtstack.chunjun.cdc.CdcConf;
import com.dtstack.chunjun.mapping.NameMappingConf;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ContentConfTest {

    /** Should return a string that contains all the fields of the class */
    @Test
    public void toStringShouldReturnAStringThatContainsAllTheFieldsOfTheClass() {
        ContentConf contentConf = new ContentConf();
        contentConf.setReader(new OperatorConf());
        contentConf.setWriter(new OperatorConf());
        contentConf.setTransformer(new TransformerConf());
        contentConf.setNameMapping(new NameMappingConf());
        contentConf.setRestoration(new CdcConf());

        String toString = contentConf.toString();

        assertTrue(toString.contains("reader"));
        assertTrue(toString.contains("writer"));
        assertTrue(toString.contains("transformer"));
        assertTrue(toString.contains("nameMapping"));
        assertTrue(toString.contains("restoration"));
    }

    /** Should return the nameMapping when the nameMapping is not null */
    @Test
    public void getNameMappingWhenNameMappingIsNotNull() {
        ContentConf contentConf = new ContentConf();
        NameMappingConf nameMappingConf = new NameMappingConf();
        contentConf.setNameMapping(nameMappingConf);
        assertEquals(nameMappingConf, contentConf.getNameMapping());
    }

    /** Should return null when the nameMapping is null */
    @Test
    public void getNameMappingWhenNameMappingIsNull() {
        ContentConf contentConf = new ContentConf();
        assertNull(contentConf.getNameMapping());
    }

    /** Should return the restoration */
    @Test
    public void getRestorationShouldReturnTheRestoration() {
        ContentConf contentConf = new ContentConf();
        CdcConf cdcConf = new CdcConf();
        contentConf.setRestoration(cdcConf);
        assertEquals(cdcConf, contentConf.getRestoration());
    }

    /** Should return the transformer */
    @Test
    public void getTransformerShouldReturnTheTransformer() {
        ContentConf contentConf = new ContentConf();
        TransformerConf transformerConf = new TransformerConf();
        contentConf.setTransformer(transformerConf);
        assertEquals(transformerConf, contentConf.getTransformer());
    }

    /** Should return the writer */
    @Test
    public void getWriterShouldReturnTheWriter() {
        ContentConf contentConf = new ContentConf();
        OperatorConf operatorConf = new OperatorConf();
        contentConf.setWriter(operatorConf);
        assertEquals(operatorConf, contentConf.getWriter());
    }

    /** Should return the reader */
    @Test
    public void getReaderShouldReturnTheReader() {
        ContentConf contentConf = new ContentConf();
        OperatorConf reader = new OperatorConf();
        contentConf.setReader(reader);
        assertEquals(reader, contentConf.getReader());
    }
}
