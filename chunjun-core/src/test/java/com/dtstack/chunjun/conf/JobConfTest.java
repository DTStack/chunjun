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
import com.dtstack.chunjun.mapping.MappingConf;

import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.LinkedList;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class JobConfTest {

    /** Should return a string that contains content */
    @Test
    public void toStringShouldReturnAStringThatContainsContent() {
        JobConf jobConf = new JobConf();
        LinkedList<ContentConf> content = new LinkedList<>();
        content.add(new ContentConf());
        jobConf.setContent(content);
        assertThat(jobConf.toString(), containsString("content"));
    }

    /** Should return a string that contains setting */
    @Test
    public void toStringShouldReturnAStringThatContainsSetting() {
        JobConf jobConf = new JobConf();
        jobConf.setSetting(new SettingConf());
        assertThat(jobConf.toString(), containsString("setting"));
    }

    /** Should return the writer of the first content */
    @Test
    public void getWriterShouldReturnTheWriterOfTheFirstContent() {
        JobConf jobConf = new JobConf();
        ContentConf contentConf = new ContentConf();
        OperatorConf writer = new OperatorConf();
        contentConf.setWriter(writer);
        jobConf.setContent(
                new LinkedList<ContentConf>() {
                    {
                        add(contentConf);
                    }
                });

        assertEquals(writer, jobConf.getWriter());
    }

    /** Should return the reader of the first content */
    @Test
    public void getReaderShouldReturnTheReaderOfTheFirstContent() {
        JobConf jobConf = new JobConf();
        ContentConf contentConf = new ContentConf();
        OperatorConf reader = new OperatorConf();
        contentConf.setReader(reader);
        jobConf.setContent(
                new LinkedList<ContentConf>() {
                    {
                        add(contentConf);
                    }
                });

        assertEquals(reader, jobConf.getReader());
    }

    /** Should return the name mapping when the content is not empty */
    @Test
    public void getNameMappingWhenContentIsNotEmpty() {
        LinkedHashMap<String, String> identifierMappings = new LinkedHashMap<>();
        identifierMappings.put("dujie", "test_${dataBaseName}");
        identifierMappings.put("dujie.*", "test_${dataBaseName}.${tableName}");
        LinkedHashMap<String, String> columnTypeMappings = new LinkedHashMap<>();
        columnTypeMappings.put("int", "varchar(255)");
        MappingConf nameMappingConf = new MappingConf(identifierMappings, columnTypeMappings);
        JobConf jobConf = new JobConf();
        ContentConf contentConf = new ContentConf();
        contentConf.setNameMapping(nameMappingConf);
        LinkedList<ContentConf> content = new LinkedList<>();
        content.add(contentConf);
        jobConf.setContent(content);

        assertEquals(nameMappingConf, jobConf.getNameMapping());
    }

    /** Should return the setting */
    @Test
    public void getSettingShouldReturnTheSetting() {
        JobConf jobConf = new JobConf();
        SettingConf settingConf = new SettingConf();
        jobConf.setSetting(settingConf);
        assertEquals(settingConf, jobConf.getSetting());
    }

    /** Should return the content */
    @Test
    public void getContentShouldReturnTheContent() {
        JobConf jobConf = new JobConf();
        LinkedList<ContentConf> content = new LinkedList<>();
        jobConf.setContent(content);
        assertEquals(content, jobConf.getContent());
    }

    /** Should return the cdc configuration */
    @Test
    public void getCdcConfShouldReturnTheCdcConfiguration() {
        JobConf jobConf = new JobConf();
        LinkedList<ContentConf> content = new LinkedList<>();
        ContentConf contentConf = new ContentConf();
        CdcConf cdcConf = new CdcConf();
        contentConf.setRestoration(cdcConf);
        content.add(contentConf);
        jobConf.setContent(content);

        CdcConf result = jobConf.getCdcConf();

        assertEquals(cdcConf, result);
    }

    /** Should return the transformer of the first content */
    @Test
    public void getTransformerShouldReturnTheTransformerOfTheFirstContent() {
        JobConf jobConf = new JobConf();
        ContentConf contentConf = new ContentConf();
        TransformerConf transformerConf = new TransformerConf();
        contentConf.setTransformer(transformerConf);
        jobConf.setContent(
                new LinkedList<ContentConf>() {
                    {
                        add(contentConf);
                    }
                });

        assertEquals(transformerConf, jobConf.getTransformer());
    }
}
