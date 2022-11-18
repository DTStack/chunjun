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
import com.dtstack.chunjun.mapping.MappingConfig;

import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.LinkedList;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class JobConfigTest {

    /** Should return a string that contains content */
    @Test
    public void toStringShouldReturnAStringThatContainsContent() {
        JobConfig jobConfig = new JobConfig();
        LinkedList<ContentConfig> content = new LinkedList<>();
        content.add(new ContentConfig());
        jobConfig.setContent(content);
        assertThat(jobConfig.toString(), containsString("content"));
    }

    /** Should return a string that contains setting */
    @Test
    public void toStringShouldReturnAStringThatContainsSetting() {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setSetting(new SettingConfig());
        assertThat(jobConfig.toString(), containsString("setting"));
    }

    /** Should return the writer of the first content */
    @Test
    public void getWriterShouldReturnTheWriterOfTheFirstContent() {
        JobConfig jobConfig = new JobConfig();
        ContentConfig contentConfig = new ContentConfig();
        OperatorConfig writer = new OperatorConfig();
        contentConfig.setWriter(writer);
        jobConfig.setContent(
                new LinkedList<ContentConfig>() {
                    {
                        add(contentConfig);
                    }
                });

        assertEquals(writer, jobConfig.getWriter());
    }

    /** Should return the reader of the first content */
    @Test
    public void getReaderShouldReturnTheReaderOfTheFirstContent() {
        JobConfig jobConfig = new JobConfig();
        ContentConfig contentConfig = new ContentConfig();
        OperatorConfig reader = new OperatorConfig();
        contentConfig.setReader(reader);
        jobConfig.setContent(
                new LinkedList<ContentConfig>() {
                    {
                        add(contentConfig);
                    }
                });

        assertEquals(reader, jobConfig.getReader());
    }

    /** Should return the name mapping when the content is not empty */
    @Test
    public void getNameMappingWhenContentIsNotEmpty() {
        LinkedHashMap<String, String> identifierMappings = new LinkedHashMap<>();
        identifierMappings.put("dujie", "test_${dataBaseName}");
        identifierMappings.put("dujie.*", "test_${dataBaseName}.${tableName}");
        LinkedHashMap<String, String> columnTypeMappings = new LinkedHashMap<>();
        columnTypeMappings.put("int", "varchar(255)");
        MappingConfig nameMappingConfig = new MappingConfig(identifierMappings, columnTypeMappings);
        JobConfig jobConfig = new JobConfig();
        ContentConfig contentConfig = new ContentConfig();
        contentConfig.setNameMapping(nameMappingConfig);
        LinkedList<ContentConfig> content = new LinkedList<>();
        content.add(contentConfig);
        jobConfig.setContent(content);

        assertEquals(nameMappingConfig, jobConfig.getNameMapping());
    }

    /** Should return the setting */
    @Test
    public void getSettingShouldReturnTheSetting() {
        JobConfig jobConfig = new JobConfig();
        SettingConfig settingConfig = new SettingConfig();
        jobConfig.setSetting(settingConfig);
        assertEquals(settingConfig, jobConfig.getSetting());
    }

    /** Should return the content */
    @Test
    public void getContentShouldReturnTheContent() {
        JobConfig jobConfig = new JobConfig();
        LinkedList<ContentConfig> content = new LinkedList<>();
        jobConfig.setContent(content);
        assertEquals(content, jobConfig.getContent());
    }

    /** Should return the cdc configuration */
    @Test
    public void getCdcConfShouldReturnTheCdcConfiguration() {
        JobConfig jobConfig = new JobConfig();
        LinkedList<ContentConfig> content = new LinkedList<>();
        ContentConfig contentConfig = new ContentConfig();
        CdcConfig cdcConfig = new CdcConfig();
        contentConfig.setRestoration(cdcConfig);
        content.add(contentConfig);
        jobConfig.setContent(content);

        CdcConfig result = jobConfig.getCdcConf();

        assertEquals(cdcConfig, result);
    }

    /** Should return the transformer of the first content */
    @Test
    public void getTransformerShouldReturnTheTransformerOfTheFirstContent() {
        JobConfig jobConfig = new JobConfig();
        ContentConfig contentConfig = new ContentConfig();
        TransformerConfig transformerConfig = new TransformerConfig();
        contentConfig.setTransformer(transformerConfig);
        jobConfig.setContent(
                new LinkedList<ContentConfig>() {
                    {
                        add(contentConfig);
                    }
                });

        assertEquals(transformerConfig, jobConfig.getTransformer());
    }
}
