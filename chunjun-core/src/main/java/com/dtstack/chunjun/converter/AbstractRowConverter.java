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

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.conf.FlinkxCommonConf;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.enums.ColumnType;
import com.dtstack.chunjun.util.DateUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Converter that is responsible to convert between JDBC object and Flink SQL internal data
 * structure {@link RowData}.
 */

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/10
 */
public abstract class AbstractRowConverter<SourceT, LookupT, SinkT, T> implements Serializable {
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    private static final long serialVersionUID = 1L;
    protected RowType rowType;
    protected ArrayList<IDeserializationConverter> toInternalConverters;
    protected ArrayList<ISerializationConverter> toExternalConverters;
    protected LogicalType[] fieldTypes;
    protected FlinkxCommonConf commonConf;

    public AbstractRowConverter() {}

    public AbstractRowConverter(RowType rowType) {
        this(rowType.getFieldCount());
        this.rowType = checkNotNull(rowType);
        this.fieldTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
    }

    public AbstractRowConverter(RowType rowType, FlinkxCommonConf commonConf) {
        this(rowType.getFieldCount());
        this.rowType = checkNotNull(rowType);
        this.fieldTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        this.commonConf = commonConf;
    }

    public AbstractRowConverter(int converterSize) {
        this.toInternalConverters = new ArrayList<>(converterSize);
        this.toExternalConverters = new ArrayList<>(converterSize);
    }

    protected IDeserializationConverter wrapIntoNullableInternalConverter(
            IDeserializationConverter IDeserializationConverter) {
        return val -> {
            if (val == null) {
                return null;
            } else {
                try {
                    return IDeserializationConverter.deserialize(val);
                } catch (Exception e) {
                    LOG.error("value [{}] convent failed ", val);
                    throw e;
                }
            }
        };
    }

    /**
     * 组装字段属性，常量、format、等等
     *
     * @param fieldConf
     * @param baseColumn
     * @return
     */
    protected AbstractBaseColumn assembleFieldProps(
            FieldConf fieldConf, AbstractBaseColumn baseColumn) {
        String format = fieldConf.getFormat();
        String parseFormat = fieldConf.getParseFormat();
        if (StringUtils.isNotBlank(fieldConf.getValue())) {
            String type = fieldConf.getType();
            if ((ColumnType.isStringType(type) || ColumnType.isTimeType(type))
                    && StringUtils.isNotBlank(format)) {
                SimpleDateFormat parseDateFormat = null;
                if (StringUtils.isNotBlank(parseFormat)) {
                    parseDateFormat = new SimpleDateFormat(parseFormat);
                }
                baseColumn =
                        new StringColumn(
                                String.valueOf(
                                        DateUtil.columnToDate(fieldConf.getValue(), parseDateFormat)
                                                .getTime()),
                                format);
            } else {
                baseColumn = new StringColumn(fieldConf.getValue(), format);
            }
        } else if (StringUtils.isNotBlank(format)) {
            baseColumn =
                    new StringColumn(
                            getMilliSecondsWithParseFormat(
                                    baseColumn.asString(), parseFormat, format),
                            format);
        }
        return baseColumn;
    }

    /** Convert val from timestampString to longString with parseFormat and */
    public String getMilliSecondsWithParseFormat(String val, String parseFormat, String format) {
        if (StringUtils.isNotBlank(parseFormat) && val != null) {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(parseFormat);
            try {
                return String.valueOf(simpleDateFormat.parse(val).getTime());
            } catch (ParseException e) {
                LOG.warn(
                        String.format(
                                "Cannot parse val %s with the given parseFormat[%s],try parsing with format[%s]",
                                val, parseFormat, format),
                        e);
                try {
                    simpleDateFormat = new SimpleDateFormat(format);
                    return String.valueOf(simpleDateFormat.parse(val).getTime());
                } catch (ParseException parseException) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Cannot parse val %s with the given parseFormat[%s] and format[%s]",
                                    val, parseFormat, format));
                }
            }
        }
        return val;
    }

    protected ISerializationConverter<SinkT> wrapIntoNullableExternalConverter(
            ISerializationConverter<SinkT> ISerializationConverter, T type) {
        return null;
    }

    /**
     * Convert data retrieved from {@link ResultSet} to internal {@link RowData}.
     *
     * @param input from JDBC
     */
    public abstract RowData toInternal(SourceT input) throws Exception;

    /**
     * @param input input
     * @return RowData
     * @throws Exception Exception
     */
    public RowData toInternalLookup(LookupT input) throws Exception {
        throw new RuntimeException("Subclass need rewriting");
    }
    /**
     * BinaryRowData
     *
     * @param rowData rowData
     * @param output output
     * @return return
     */
    public abstract SinkT toExternal(RowData rowData, SinkT output) throws Exception;

    /**
     * 将外部数据库类型转换为flink内部类型
     *
     * @param type type
     * @return return
     */
    protected IDeserializationConverter createInternalConverter(T type) {
        return null;
    }

    /**
     * 将flink内部的数据类型转换为外部数据库系统类型
     *
     * @param type type
     * @return return
     */
    protected ISerializationConverter createExternalConverter(T type) {
        return null;
    }

    public FlinkxCommonConf getCommonConf() {
        return commonConf;
    }

    public void setCommonConf(FlinkxCommonConf commonConf) {
        this.commonConf = commonConf;
    }
}
