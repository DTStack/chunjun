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

package com.dtstack.chunjun.converter;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.element.AbstractBaseColumn;

import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AbstractRowConverterTest {
    private AbstractRowConverter<String, String, String, String> rowConverter;

    @BeforeEach
    void setUp() {
        rowConverter =
                new AbstractRowConverter<String, String, String, String>(null) {
                    @Override
                    public RowData toInternal(String input) throws Exception {
                        return null;
                    }

                    @Override
                    public String toExternal(RowData rowData, String output) throws Exception {
                        return null;
                    }
                };
    }

    @Test
    @DisplayName("can't parse date")
    void assembleFieldPropsWhenValueIsNotBlankThenReturnStringColumn() {
        FieldConfig fieldConfig = mock(FieldConfig.class);
        when(fieldConfig.getValue()).thenReturn("value");
        when(fieldConfig.getType()).thenReturn(TypeConfig.fromString("string"));
        when(fieldConfig.getFormat()).thenReturn("format");

        AbstractBaseColumn baseColumn = mock(AbstractBaseColumn.class);
        assertThrows(
                RuntimeException.class,
                () -> rowConverter.assembleFieldProps(fieldConfig, baseColumn));
    }

    @Test
    @DisplayName("Should return stringcolumn when the value is blank and format is not blank")
    void assembleFieldPropsWhenValueIsBlankAndFormatIsNotBlankThenReturnStringColumn() {
        FieldConfig fieldConfig = mock(FieldConfig.class);
        when(fieldConfig.getValue()).thenReturn("");
        when(fieldConfig.getFormat()).thenReturn("yyyy-MM-dd HH:mm:ss");
        AbstractBaseColumn baseColumn = mock(AbstractBaseColumn.class);
        AbstractBaseColumn result = rowConverter.assembleFieldProps(fieldConfig, baseColumn);
        assertNotNull(result);
    }

    @Test
    @DisplayName("Should return a converter that returns null when the input is null")
    void wrapIntoNullableInternalConverterWhenInputIsNullThenReturnNull() throws Exception {
        IDeserializationConverter deserializationConverter = mock(IDeserializationConverter.class);
        IDeserializationConverter nullableDeserializationConverter =
                rowConverter.wrapIntoNullableInternalConverter(deserializationConverter);

        Object result = nullableDeserializationConverter.deserialize(null);

        assertNull(result);
    }

    @Test
    @DisplayName(
            "Should return a converter that returns the converted value when the input is not null")
    void wrapIntoNullableInternalConverterWhenInputIsNotNullThenReturnTheConvertedValue()
            throws Exception {
        IDeserializationConverter mockDeserializationConverter =
                mock(IDeserializationConverter.class);
        when(mockDeserializationConverter.deserialize("input")).thenReturn("output");

        IDeserializationConverter converter =
                rowConverter.wrapIntoNullableInternalConverter(mockDeserializationConverter);

        assertEquals("output", converter.deserialize("input"));
    }

    @Test
    @DisplayName("Should return null when the input is null")
    void wrapIntoNullableExternalConverterWhenInputIsNullThenReturnNull() {
        ISerializationConverter<String> serializationConverter =
                mock(ISerializationConverter.class);
        ISerializationConverter<String> result =
                rowConverter.wrapIntoNullableExternalConverter(serializationConverter, null);
        assertNull(result);
    }

    @Test
    @DisplayName("Should set the commonconf when the commonconf is not null")
    void setCommonConfWhenCommonConfIsNotNull() {
        CommonConfig commonConfig = new CommonConfig();
        rowConverter.setCommonConfig(commonConfig);

        assertEquals(commonConfig, rowConverter.getCommonConfig());
    }

    @Test
    @DisplayName("Should not set the commonconf when the commonconf is null")
    void setCommonConfWhenCommonConfIsNull() {

        rowConverter.setCommonConfig(null);

        assertNull(rowConverter.getCommonConfig());
    }

    @Test
    @DisplayName("Should return the milliseconds")
    void
            getMilliSecondsWithParseFormatWhenParseFormatIsNotBlankAndValIsNotNullThenReturnTheMilliseconds() {
        String val = "2020-01-01";
        String parseFormat = "yyyy-MM-dd";
        String format = "yyyy-MM-dd HH:mm:ss";
        String expected = "1577808000000";
        String actual = rowConverter.getMilliSecondsWithParseFormat(val, parseFormat, format);
        assertEquals(expected, actual);
    }
}
