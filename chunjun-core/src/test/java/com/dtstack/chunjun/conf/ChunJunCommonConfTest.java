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

public class ChunJunCommonConfTest {

    /** Should return a string with all the fields */
    @Test
    public void toStringShouldReturnAStringWithAllTheFields() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setSpeedBytes(1);
        chunJunCommonConf.setErrorRecord(2);
        chunJunCommonConf.setErrorPercentage(3);
        chunJunCommonConf.setDirtyDataPath("dirtyDataPath");
        Map<String, Object> dirtyDataHadoopConf = new HashMap<>();
        dirtyDataHadoopConf.put("key", "value");
        chunJunCommonConf.setDirtyDataHadoopConf(dirtyDataHadoopConf);
        chunJunCommonConf.setFieldNameList(null);
        chunJunCommonConf.setCheckFormat(true);
        chunJunCommonConf.setParallelism(4);
        chunJunCommonConf.setColumn(null);
        chunJunCommonConf.setBatchSize(5);
        chunJunCommonConf.setFlushIntervalMills(6L);
        chunJunCommonConf.setMetricPluginRoot("metricPluginRoot");
        chunJunCommonConf.setMetricPluginName("metricPluginName");
        Map<String, Object> metricProps = new HashMap<>();
        metricProps.put("key", "value");
        chunJunCommonConf.setMetricProps(metricProps);

        String expected =
                "ChunJunCommonConf{speedBytes=1, errorRecord=2, errorPercentage=3, dirtyDataPath='dirtyDataPath', dirtyDataHadoopConf={key=value}, fieldNameList=null, checkFormat=true, parallelism=4, column=null, batchSize=5, executeDdlAble=false, flushIntervalMills=6, metricPluginRoot='metricPluginRoot', metricPluginName='metricPluginName', metricProps={key=value}, savePointPath=null}";

        assertEquals(expected, chunJunCommonConf.toString());
    }

    /** Should return a list of columns when the column is not null */
    @Test
    public void getColumnWhenColumnIsNotNullThenReturnListOfColumns() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        FieldConf fieldConf = new FieldConf();
        fieldConf.setName("name");
        fieldConf.setType("type");
        fieldConf.setIndex(1);
        fieldConf.setValue("value");
        fieldConf.setFormat("format");
        fieldConf.setSplitter("splitter");
        fieldConf.setPart(true);
        fieldConf.setNotNull(true);
        fieldConf.setLength(1);
        fieldConf.setParseFormat("parseFormat");

        List<FieldConf> column = new ArrayList<>();
        column.add(fieldConf);

        chunJunCommonConf.setColumn(column);

        assertEquals(column, chunJunCommonConf.getColumn());
    }

    /** Should return the savePointPath when the savePointPath is not null */
    @Test
    public void getSavePointPathWhenSavePointPathIsNotNull() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setSavePointPath("/tmp/savepoint");
        assertEquals("/tmp/savepoint", chunJunCommonConf.getSavePointPath());
    }

    /** Should return the flushIntervalMills when the flushIntervalMills is set */
    @Test
    public void getFlushIntervalMillsWhenFlushIntervalMillsIsSet() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setFlushIntervalMills(100L);
        assertEquals(100L, chunJunCommonConf.getFlushIntervalMills());
    }

    /** Should return 10000L when the flushIntervalMills is not set */
    @Test
    public void getFlushIntervalMillsWhenFlushIntervalMillsIsNotSet() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        assertEquals(10000L, chunJunCommonConf.getFlushIntervalMills());
    }

    /** Should return 1 when the batchSize is not set */
    @Test
    public void getBatchSizeWhenBatchSizeIsNotSet() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        assertEquals(1, chunJunCommonConf.getBatchSize());
    }

    /** Should return the batch size when the batch size is set */
    @Test
    public void getBatchSizeWhenBatchSizeIsSet() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setBatchSize(10);
        assertEquals(10, chunJunCommonConf.getBatchSize());
    }

    /** Should return parallelism when parallelism is not null */
    @Test
    public void getParallelismWhenParallelismIsNotNullThenReturnParallelism() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setParallelism(1);
        assertEquals(1, chunJunCommonConf.getParallelism().intValue());
    }

    /** Should return true when checkFormat is true */
    @Test
    public void isCheckFormatWhenCheckFormatIsTrueThenReturnTrue() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setCheckFormat(true);
        assertTrue(chunJunCommonConf.isCheckFormat());
    }

    /** Should return false when checkFormat is false */
    @Test
    public void isCheckFormatWhenCheckFormatIsFalseThenReturnFalse() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setCheckFormat(false);
        assertFalse(chunJunCommonConf.isCheckFormat());
    }

    /** Should return the fieldNameList when the fieldNameList is not null */
    @Test
    public void getFieldNameListWhenFieldNameListIsNotNullThenReturnTheFieldNameList() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setFieldNameList(null);
        assertNull(chunJunCommonConf.getFieldNameList());
    }

    /** Should return the dirtyDataHadoopConf when the dirtyDataHadoopConf is not null */
    @Test
    public void getDirtyDataHadoopConfWhenDirtyDataHadoopConfIsNotNull() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setDirtyDataHadoopConf(null);
        assertNull(chunJunCommonConf.getDirtyDataHadoopConf());
    }

    /** Should return the dirtyDataPath when the dirtyDataPath is not null */
    @Test
    public void getDirtyDataPathWhenDirtyDataPathIsNotNull() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setDirtyDataPath("/tmp/dirtyDataPath");
        assertEquals("/tmp/dirtyDataPath", chunJunCommonConf.getDirtyDataPath());
    }

    /** Should return metricProps when metricProps is not null */
    @Test
    public void getMetricPropsWhenMetricPropsIsNotNull() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        Map<String, Object> metricProps = new HashMap<>();
        metricProps.put("key", "value");
        chunJunCommonConf.setMetricProps(metricProps);
        assertNotNull(chunJunCommonConf.getMetricProps());
    }

    /** Should return 0 when speedBytes is 0 */
    @Test
    public void getSpeedBytesWhenSpeedBytesIs0() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setSpeedBytes(0);
        assertEquals(0, chunJunCommonConf.getSpeedBytes());
    }

    /** Should return 1 when speedBytes is 1 */
    @Test
    public void getSpeedBytesWhenSpeedBytesIs1() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setSpeedBytes(1);
        assertEquals(1, chunJunCommonConf.getSpeedBytes());
    }

    /** Should return the errorRecord when the errorRecord is set */
    @Test
    public void getErrorRecordWhenErrorRecordIsSet() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setErrorRecord(10);
        assertEquals(10, chunJunCommonConf.getErrorRecord());
    }

    /** Should return 0 when the errorRecord is not set */
    @Test
    public void getErrorRecordWhenErrorRecordIsNotSet() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        assertEquals(0, chunJunCommonConf.getErrorRecord());
    }

    /** Should return objectSizeCalculator when rowSizeCalculatorType is null */
    @Test
    public void
            getRowSizeCalculatorTypeWhenRowSizeCalculatorTypeIsNullThenReturnObjectSizeCalculator() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        assertEquals("objectSizeCalculator", chunJunCommonConf.getRowSizeCalculatorType());
    }

    /** Should return rowSizeCalculatorType when rowSizeCalculatorType is not null */
    @Test
    public void
            getRowSizeCalculatorTypeWhenRowSizeCalculatorTypeIsNotNullThenReturnRowSizeCalculatorType() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setRowSizeCalculatorType("rowSizeCalculatorType");
        assertEquals("rowSizeCalculatorType", chunJunCommonConf.getRowSizeCalculatorType());
    }

    /** Should return the error percentage */
    @Test
    public void getErrorPercentageShouldReturnTheErrorPercentage() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setErrorPercentage(10);
        assertEquals(10, chunJunCommonConf.getErrorPercentage());
    }

    /** Should return prometheus when metricPluginName is null */
    @Test
    public void getMetricPluginNameWhenMetricPluginNameIsNullThenReturnPrometheus() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        assertEquals("prometheus", chunJunCommonConf.getMetricPluginName());
    }

    /** Should return metricPluginName when metricPluginName is not null */
    @Test
    public void getMetricPluginNameWhenMetricPluginNameIsNotNullThenReturnMetricPluginName() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setMetricPluginName("prometheus");
        assertEquals("prometheus", chunJunCommonConf.getMetricPluginName());
    }

    /** Should return the metricPluginRoot when the metricPluginRoot is not null */
    @Test
    public void getMetricPluginRootWhenMetricPluginRootIsNotNull() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setMetricPluginRoot("/tmp/metric");
        assertEquals("/tmp/metric", chunJunCommonConf.getMetricPluginRoot());
    }

    /** Should return the default value when the metricPluginRoot is null */
    @Test
    public void getMetricPluginRootWhenMetricPluginRootIsNull() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        assertNull(chunJunCommonConf.getMetricPluginRoot());
    }

    /** Should return the semantic when the semantic is not null */
    @Test
    public void getSemanticWhenSemanticIsNotNullThenReturnSemantic() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        chunJunCommonConf.setSemantic("at-least-once");
        assertEquals("at-least-once", chunJunCommonConf.getSemantic());
    }

    /** Should return at-least-once when the semantic is null */
    @Test
    public void getSemanticWhenSemanticIsNullThenReturnAtLeastOnce() {
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        assertEquals("at-least-once", chunJunCommonConf.getSemantic());
    }
}
