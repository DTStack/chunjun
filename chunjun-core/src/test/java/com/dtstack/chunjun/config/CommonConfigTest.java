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

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CommonConfigTest {

    /** Should return a string with all the fields */
    @Test
    public void toStringShouldReturnAStringWithAllTheFields() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setSpeedBytes(1);
        commonConfig.setErrorRecord(2);
        commonConfig.setErrorPercentage(3);
        commonConfig.setDirtyDataPath("dirtyDataPath");
        Map<String, Object> dirtyDataHadoopConf = new HashMap<>();
        dirtyDataHadoopConf.put("key", "value");
        commonConfig.setDirtyDataHadoopConf(dirtyDataHadoopConf);
        commonConfig.setFieldNameList(null);
        commonConfig.setCheckFormat(true);
        commonConfig.setParallelism(4);
        commonConfig.setColumn(null);
        commonConfig.setBatchSize(5);
        commonConfig.setFlushIntervalMills(6L);
        commonConfig.setMetricPluginRoot("metricPluginRoot");
        commonConfig.setMetricPluginName("metricPluginName");
        Map<String, Object> metricProps = new HashMap<>();
        metricProps.put("key", "value");
        commonConfig.setMetricProps(metricProps);

        String expected =
                "CommonConfig(speedBytes=1, errorRecord=2, errorPercentage=3, dirtyDataPath=dirtyDataPath, dirtyDataHadoopConf={key=value}, fieldNameList=null, checkFormat=true, parallelism=4, column=null, batchSize=5, flushIntervalMills=6, executeDdlAble=false, savePointPath=null, metricPluginRoot=metricPluginRoot, metricPluginName=metricPluginName, rowSizeCalculatorType=objectSizeCalculator, semantic=at-least-once, metricProps={key=value})";

        assertEquals(expected, commonConfig.toString());
    }

    /** Should return a list of columns when the column is not null */
    @Test
    public void getColumnWhenColumnIsNotNullThenReturnListOfColumns() {
        CommonConfig commonConfig = new CommonConfig();
        FieldConfig fieldConfig = new FieldConfig();
        fieldConfig.setName("name");
        fieldConfig.setType(TypeConfig.fromString("type"));
        fieldConfig.setIndex(1);
        fieldConfig.setValue("value");
        fieldConfig.setFormat("format");
        fieldConfig.setSplitter("splitter");
        fieldConfig.setIsPart(true);
        fieldConfig.setNotNull(true);
        fieldConfig.setParseFormat("parseFormat");

        List<FieldConfig> column = new ArrayList<>();
        column.add(fieldConfig);

        commonConfig.setColumn(column);

        assertEquals(column, commonConfig.getColumn());
    }

    /** Should return the savePointPath when the savePointPath is not null */
    @Test
    public void getSavePointPathWhenSavePointPathIsNotNull() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setSavePointPath("/tmp/savepoint");
        assertEquals("/tmp/savepoint", commonConfig.getSavePointPath());
    }

    /** Should return the flushIntervalMills when the flushIntervalMills is set */
    @Test
    public void getFlushIntervalMillsWhenFlushIntervalMillsIsSet() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setFlushIntervalMills(100L);
        assertEquals(100L, commonConfig.getFlushIntervalMills());
    }

    /** Should return 10000L when the flushIntervalMills is not set */
    @Test
    public void getFlushIntervalMillsWhenFlushIntervalMillsIsNotSet() {
        CommonConfig commonConfig = new CommonConfig();
        assertEquals(10000L, commonConfig.getFlushIntervalMills());
    }

    /** Should return 1 when the batchSize is not set */
    @Test
    public void getBatchSizeWhenBatchSizeIsNotSet() {
        CommonConfig commonConfig = new CommonConfig();
        assertEquals(1, commonConfig.getBatchSize());
    }

    /** Should return the batch size when the batch size is set */
    @Test
    public void getBatchSizeWhenBatchSizeIsSet() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setBatchSize(10);
        assertEquals(10, commonConfig.getBatchSize());
    }

    /** Should return parallelism when parallelism is not null */
    @Test
    public void getParallelismWhenParallelismIsNotNullThenReturnParallelism() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setParallelism(1);
        assertEquals(1, commonConfig.getParallelism().intValue());
    }

    /** Should return true when checkFormat is true */
    @Test
    public void isCheckFormatWhenCheckFormatIsTrueThenReturnTrue() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setCheckFormat(true);
        assertTrue(commonConfig.isCheckFormat());
    }

    /** Should return false when checkFormat is false */
    @Test
    public void isCheckFormatWhenCheckFormatIsFalseThenReturnFalse() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setCheckFormat(false);
        assertFalse(commonConfig.isCheckFormat());
    }

    /** Should return the fieldNameList when the fieldNameList is not null */
    @Test
    public void getFieldNameListWhenFieldNameListIsNotNullThenReturnTheFieldNameList() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setFieldNameList(null);
        assertNull(commonConfig.getFieldNameList());
    }

    /** Should return the dirtyDataHadoopConf when the dirtyDataHadoopConf is not null */
    @Test
    public void getDirtyDataHadoopConfWhenDirtyDataHadoopConfIsNotNull() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setDirtyDataHadoopConf(null);
        assertNull(commonConfig.getDirtyDataHadoopConf());
    }

    /** Should return the dirtyDataPath when the dirtyDataPath is not null */
    @Test
    public void getDirtyDataPathWhenDirtyDataPathIsNotNull() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setDirtyDataPath("/tmp/dirtyDataPath");
        assertEquals("/tmp/dirtyDataPath", commonConfig.getDirtyDataPath());
    }

    /** Should return metricProps when metricProps is not null */
    @Test
    public void getMetricPropsWhenMetricPropsIsNotNull() {
        CommonConfig commonConfig = new CommonConfig();
        Map<String, Object> metricProps = new HashMap<>();
        metricProps.put("key", "value");
        commonConfig.setMetricProps(metricProps);
        assertNotNull(commonConfig.getMetricProps());
    }

    /** Should return 0 when speedBytes is 0 */
    @Test
    public void getSpeedBytesWhenSpeedBytesIs0() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setSpeedBytes(0);
        assertEquals(0, commonConfig.getSpeedBytes());
    }

    /** Should return 1 when speedBytes is 1 */
    @Test
    public void getSpeedBytesWhenSpeedBytesIs1() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setSpeedBytes(1);
        assertEquals(1, commonConfig.getSpeedBytes());
    }

    /** Should return the errorRecord when the errorRecord is set */
    @Test
    public void getErrorRecordWhenErrorRecordIsSet() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setErrorRecord(10);
        assertEquals(10, commonConfig.getErrorRecord());
    }

    /** Should return 0 when the errorRecord is not set */
    @Test
    public void getErrorRecordWhenErrorRecordIsNotSet() {
        CommonConfig commonConfig = new CommonConfig();
        assertEquals(0, commonConfig.getErrorRecord());
    }

    /** Should return objectSizeCalculator when rowSizeCalculatorType is null */
    @Test
    public void
            getRowSizeCalculatorTypeWhenRowSizeCalculatorTypeIsNullThenReturnObjectSizeCalculator() {
        CommonConfig commonConfig = new CommonConfig();
        assertEquals("objectSizeCalculator", commonConfig.getRowSizeCalculatorType());
    }

    /** Should return rowSizeCalculatorType when rowSizeCalculatorType is not null */
    @Test
    public void
            getRowSizeCalculatorTypeWhenRowSizeCalculatorTypeIsNotNullThenReturnRowSizeCalculatorType() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setRowSizeCalculatorType("rowSizeCalculatorType");
        assertEquals("rowSizeCalculatorType", commonConfig.getRowSizeCalculatorType());
    }

    /** Should return the error percentage */
    @Test
    public void getErrorPercentageShouldReturnTheErrorPercentage() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setErrorPercentage(10);
        assertEquals(10, commonConfig.getErrorPercentage());
    }

    /** Should return prometheus when metricPluginName is null */
    @Test
    public void getMetricPluginNameWhenMetricPluginNameIsNullThenReturnPrometheus() {
        CommonConfig commonConfig = new CommonConfig();
        assertEquals("prometheus", commonConfig.getMetricPluginName());
    }

    /** Should return metricPluginName when metricPluginName is not null */
    @Test
    public void getMetricPluginNameWhenMetricPluginNameIsNotNullThenReturnMetricPluginName() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setMetricPluginName("prometheus");
        assertEquals("prometheus", commonConfig.getMetricPluginName());
    }

    /** Should return the metricPluginRoot when the metricPluginRoot is not null */
    @Test
    public void getMetricPluginRootWhenMetricPluginRootIsNotNull() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setMetricPluginRoot("/tmp/metric");
        assertEquals("/tmp/metric", commonConfig.getMetricPluginRoot());
    }

    /** Should return the default value when the metricPluginRoot is null */
    @Test
    public void getMetricPluginRootWhenMetricPluginRootIsNull() {
        CommonConfig commonConfig = new CommonConfig();
        assertNull(commonConfig.getMetricPluginRoot());
    }

    /** Should return the semantic when the semantic is not null */
    @Test
    public void getSemanticWhenSemanticIsNotNullThenReturnSemantic() {
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setSemantic("at-least-once");
        assertEquals("at-least-once", commonConfig.getSemantic());
    }

    /** Should return at-least-once when the semantic is null */
    @Test
    public void getSemanticWhenSemanticIsNullThenReturnAtLeastOnce() {
        CommonConfig commonConfig = new CommonConfig();
        assertEquals("at-least-once", commonConfig.getSemantic());
    }
}
