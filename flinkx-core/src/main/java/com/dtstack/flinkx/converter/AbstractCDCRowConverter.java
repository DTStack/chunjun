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

package com.dtstack.flinkx.converter;

import com.dtstack.flinkx.util.SnowflakeIdWorker;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Date: 2021/04/29 Company: www.dtstack.com
 *
 * @author tudou
 */
public abstract class AbstractCDCRowConverter<SourceT, T> implements Serializable {
    protected static final long serialVersionUID = 1L;

    // times
    protected static final DateTimeFormatter SQL_TIME_FORMAT =
            (new DateTimeFormatterBuilder())
                    .appendPattern("HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter();
    protected static final DateTimeFormatter SQL_TIMESTAMP_FORMAT =
            (new DateTimeFormatterBuilder())
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(SQL_TIME_FORMAT)
                    .toFormatter();
    protected static final DateTimeFormatter ISO8601_TIMESTAMP_FORMAT =
            DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    protected static DateTimeFormatter SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(SQL_TIME_FORMAT)
                    .appendPattern("'Z'")
                    .toFormatter();
    protected static DateTimeFormatter ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral('T')
                    .append(DateTimeFormatter.ISO_LOCAL_TIME)
                    .appendPattern("'Z'")
                    .toFormatter();
    protected final Logger LOG = LoggerFactory.getLogger(getClass());
    protected final Map<String, List<IDeserializationConverter>> cdcConverterCacheMap =
            new ConcurrentHashMap<>(32);
    protected final SnowflakeIdWorker idWorker = new SnowflakeIdWorker(1, 1);
    /** pavingData 和 split 互斥 */
    protected boolean pavingData;

    protected boolean split;
    protected List<String> fieldNameList;
    protected List<IDeserializationConverter> converters;

    /**
     * 将外部数据库类型转换为flink内部类型
     *
     * @param input
     * @return
     * @throws Exception
     */
    public abstract LinkedList<RowData> toInternal(SourceT input) throws Exception;

    /**
     * 将外部数据库类型转换为flink内部类型
     *
     * @param type
     * @return
     */
    protected abstract IDeserializationConverter createInternalConverter(T type);

    protected IDeserializationConverter wrapIntoNullableInternalConverter(
            IDeserializationConverter IDeserializationConverter) {
        return val -> {
            if (val == null) {
                return null;
            } else {
                return IDeserializationConverter.deserialize(val);
            }
        };
    }

    /**
     * 根据eventType获取RowKind
     *
     * @param type
     * @return
     */
    protected RowKind getRowKindByType(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "INSERT":
            case "UPDATE":
                return RowKind.INSERT;
            case "DELETE":
                return RowKind.DELETE;
            default:
                throw new RuntimeException("unsupported eventType: " + type);
        }
    }

    /**
     * 通过convertor将map中的数据按照顺序取出并转换成对应的类型，最终设置到rowData中
     *
     * @param fieldNameList
     * @param converters
     * @param valueMap
     * @return
     */
    @SuppressWarnings("unchecked")
    protected RowData createRowDataByConverters(
            List<String> fieldNameList,
            List<IDeserializationConverter> converters,
            Map<Object, Object> valueMap)
            throws Exception {
        GenericRowData genericRowData = new GenericRowData(fieldNameList.size());
        for (int i = 0; i < fieldNameList.size(); i++) {
            String fieldName = fieldNameList.get(i);
            Object value = valueMap.get(fieldName);
            if (value != null) {
                value = converters.get(i).deserialize(value);
            }
            genericRowData.setField(i, value);
        }
        return genericRowData;
    }
}
