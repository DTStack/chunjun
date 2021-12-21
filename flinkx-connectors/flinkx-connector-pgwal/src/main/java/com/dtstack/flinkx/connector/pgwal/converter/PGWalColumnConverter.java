/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.connector.pgwal.converter;

import com.dtstack.flinkx.connector.pgwal.util.ChangeLog;
import com.dtstack.flinkx.connector.pgwal.util.ColumnInfo;
import com.dtstack.flinkx.connector.pgwal.util.PgMessageTypeEnum;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.converter.AbstractCDCRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.BytesColumn;
import com.dtstack.flinkx.element.column.MapColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.table.types.extraction.DataTypeExtractor;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.types.utils.TypeInfoDataTypeConverter;
import org.apache.flink.types.RowKind;

import com.google.common.collect.Lists;
import org.postgresql.jdbc.PgSQLXML;

import java.lang.reflect.TypeVariable;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** */
public class PGWalColumnConverter extends AbstractCDCRowConverter<ChangeLog, LogicalType> {

    private static final Map<String, LogicalType> registered = new HashMap<>();

    static {
        registered.put(
                "uuid",
                new GenericLogicalType<java.util.UUID>(
                        true,
                        "uuid",
                        Lists.newArrayList(UUID.class, String.class),
                        Lists.newArrayList(String.class)));
        registered.put(
                "xml",
                new GenericLogicalType<PgSQLXML>(
                        true,
                        "xml",
                        Lists.newArrayList(PgSQLXML.class, String.class),
                        Lists.newArrayList(String.class)));
    }

    public PGWalColumnConverter(boolean pavingData, boolean splitUpdate) {
        super.pavingData = pavingData;
        super.splitUpdate = splitUpdate;
    }

    interface DeserializationConverter<A, B> {
        B convert(A raw, Context context);

        class Context {}
    }

    @Override
    @SuppressWarnings("unchecked")
    public LinkedList<RowData> toInternal(ChangeLog entity) throws Exception {
        LinkedList<RowData> result = new LinkedList<>();
        PgMessageTypeEnum eventType = entity.getType();
        String schema = entity.getSchema();
        String table = entity.getTable();
        String key = schema + ConstantValue.POINT_SYMBOL + table;
        List<IDeserializationConverter> converters = cdcConverterCacheMap.get(key);

        List<ColumnInfo> columnList = entity.getColumnList();

        if (converters == null || needChange(entity)) {
            cdcConverterCacheMap.put(
                    key,
                    Arrays.asList(
                            columnList.stream()
                                    .map(
                                            column ->
                                                    createInternalConverter(
                                                            translateDataType(column.getType())
                                                                    .getLogicalType()))
                                    .toArray(IDeserializationConverter[]::new)));
        }

        converters = cdcConverterCacheMap.get(key);

        int size;
        if (pavingData) {
            // 5: type, schema, table, ts, opTime
            size = 5 + entity.getNewData().length + entity.getOldData().length;
        } else {
            // 7: type, schema, table, ts, opTime, before, after
            size = 7;
        }

        ColumnRowData columnRowData = new ColumnRowData(size);
        columnRowData.addField(new StringColumn(schema));
        columnRowData.addHeader(SCHEMA);
        columnRowData.addField(new StringColumn(table));
        columnRowData.addHeader(TABLE);
        columnRowData.addField(new BigDecimalColumn(super.idWorker.nextId()));
        columnRowData.addHeader(TS);
        columnRowData.addField(new TimestampColumn(entity.getTs()));
        columnRowData.addHeader(OP_TIME);

        List<Object> beforeList = Stream.of(entity.getOldData()).collect(Collectors.toList());
        List<Object> afterList = Stream.of(entity.getNewData()).collect(Collectors.toList());

        List<AbstractBaseColumn> beforeColumnList = new ArrayList<>(beforeList.size());
        List<String> beforeHeaderList =
                entity.getOldData() == null
                        ? new ArrayList<>()
                        : columnList.stream().map(ColumnInfo::getName).collect(Collectors.toList());
        List<AbstractBaseColumn> afterColumnList = new ArrayList<>(afterList.size());
        List<String> afterHeaderList =
                entity.getNewData() == null
                        ? new ArrayList<>()
                        : columnList.stream().map(ColumnInfo::getName).collect(Collectors.toList());

        if (pavingData) {
            beforeHeaderList =
                    parseColumnList(
                            converters, beforeList, beforeColumnList, beforeHeaderList, BEFORE_);
            afterHeaderList =
                    parseColumnList(
                            converters, afterList, afterColumnList, afterHeaderList, AFTER_);
        } else {
            beforeColumnList.add(
                    new MapColumn(processColumnList(entity.getColumnList(), entity.getOldData())));
            beforeHeaderList.add(BEFORE);
            afterColumnList.add(
                    new MapColumn(processColumnList(entity.getColumnList(), entity.getNewData())));
            afterHeaderList.add(AFTER);
        }

        // update类型且要拆分
        if (splitUpdate && PgMessageTypeEnum.UPDATE == entity.getType()) {
            ColumnRowData copy = columnRowData.copy();
            copy.setRowKind(RowKind.UPDATE_BEFORE);
            copy.addField(new StringColumn(RowKind.UPDATE_BEFORE.name()));
            copy.addHeader(TYPE);
            copy.addAllField(beforeColumnList);
            copy.addAllHeader(beforeHeaderList);
            result.add(copy);

            columnRowData.setRowKind(RowKind.UPDATE_AFTER);
            columnRowData.addField(new StringColumn(RowKind.UPDATE_AFTER.name()));
            columnRowData.addHeader(TYPE);
        } else {
            columnRowData.setRowKind(getRowKindByType(eventType.name()));
            columnRowData.addField(new StringColumn(eventType.name()));
            columnRowData.addHeader(TYPE);
            columnRowData.addAllField(beforeColumnList);
            columnRowData.addAllHeader(beforeHeaderList);
        }
        columnRowData.addAllField(afterColumnList);
        columnRowData.addAllHeader(afterHeaderList);

        result.add(columnRowData);
        return result;
    }

    private DataType translateDataType(String type) {
        // phase 1, direct mapping
        Map<String, DataType> typeMapping = new HashMap<>();
        typeMapping.put("int8", DataTypes.BIGINT());
        //        typeMapping.put("int2", DataTypes.SMALLINT());
        typeMapping.put("int2", DataTypes.TINYINT());
        typeMapping.put("bit", DataTypes.TINYINT());
        typeMapping.put("int4", DataTypes.INT());
        typeMapping.put("float4", DataTypes.FLOAT());
        typeMapping.put("float8", DataTypes.DOUBLE());
        typeMapping.put("date", DataTypes.DATE());
        typeMapping.put("time", DataTypes.TIME());
        typeMapping.put("timestamp", DataTypes.TIMESTAMP());

        typeMapping.put("numeric", DataTypes.DECIMAL(10, 10)); // TEST

        // phase 2, need more information to parse
        if (typeMapping.containsKey(type)) return typeMapping.get(type);

        /**
         * typeMapping.put("char(1)", DataTypes.CHAR()); typeMapping.put("varchar",
         * DataTypes.VARCHAR()); typeMapping.put("bytea", DataTypes.VARBINARY());
         * typeMapping.put("bytea", DataTypes.BINARY()); typeMapping.put("text",
         * DataTypes.VARBINARY()); typeMapping.put("bytea", DataTypes.VARBINARY());
         * typeMapping.put("bit", DataTypes.BINARY());
         */
        List<String> needMoreInfo =
                Lists.newArrayList("char", "varchar", "bytea", "text", "numeric");
        if (needMoreInfo.contains(type)) {
            return DataTypeFactory.getDataType(type);
        }

        // phase 3  extend
        /**
         * typeMapping.put("text", DataTypes.CLOB()); typeMapping.put("oid", DataTypes.BLOB());
         * typeMapping.put("numeric", DataTypes.NUMERIC()); typeMapping.put("uuid",
         * DataTypes.UUID()); typeMapping.put("numeric", DataTypes.NUMERIC());
         * typeMapping.put("json", DataTypes.JSON_OBJECT()); typeMapping.put("xml",
         * DataTypes.XML());
         */
        return new DataTypeFactoryMock().createDataType(type);
    }

    private boolean needChange(ChangeLog entity) {
        // DDL ?
        return false;
    }

    private Map<String, Object> processColumnList(List<ColumnInfo> columnList, Object[] oldData) {
        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < columnList.size(); i++) {
            result.put(columnList.get(i).getName(), oldData[i]);
        }
        return result;
    }

    /**
     * 解析CanalEntry.Column
     *
     * @param converters
     * @param entryColumnList
     * @param columnList
     * @param headerList
     * @param after
     * @return
     */
    private List<String> parseColumnList(
            List<IDeserializationConverter> converters,
            List<Object> entryColumnList,
            List<AbstractBaseColumn> columnList,
            List<String> headerList,
            String after)
            throws Exception {
        List<String> originList = new ArrayList<>();
        for (int i = 0; i < entryColumnList.size(); i++) {
            Object entryColumn = entryColumnList.get(i);
            if (entryColumn != null) {
                AbstractBaseColumn column =
                        (AbstractBaseColumn)
                                converters.get(i).deserialize(entryColumnList.get(i).toString());
                columnList.add(column);
                originList.add(after + headerList.get(i));
            }
        }
        return originList;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                //                return (DeserializationConverter<String, Boolean>) (raw, context)
                // -> null;
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case TINYINT:
                return val -> new BigDecimalColumn((String) val);
            case SMALLINT:
            case INTEGER:
                return val -> new BigDecimalColumn((String) val);
            case FLOAT:
                return val -> new BigDecimalColumn((String) val);
            case DOUBLE:
                return val -> new BigDecimalColumn((String) val);
            case BIGINT:
                return val -> new BigDecimalColumn((String) val);
            case DECIMAL:
                return val -> new BigDecimalColumn((String) val);
            case CHAR:
            case VARCHAR:
                return val -> new StringColumn((String) val);
            case DATE:
                return val ->
                        new BigDecimalColumn(
                                Date.valueOf(String.valueOf(val)).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        new BigDecimalColumn(
                                Time.valueOf(String.valueOf(val)).toLocalTime().toNanoOfDay()
                                        / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> {
                    SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    java.util.Date date;
                    try {
                        date = sf.parse((String) val);
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                    return new TimestampColumn(date.getTime());
                };
            case BINARY:
            case VARBINARY:
                return val -> new BytesColumn(((String) val).getBytes(StandardCharsets.UTF_8));
            default:
                if (type instanceof GenericLogicalType) {
                    GenericLogicalType genericLogicalType = (GenericLogicalType) type;
                    String logicalType = genericLogicalType.type();
                    switch (logicalType) {
                        case "uuid":
                            return value -> {
                                if (value instanceof String) {
                                    return new StringColumn((String) value);
                                } else if (value instanceof UUID) {
                                    return new StringColumn(((UUID) value).toString());
                                }
                                return null;
                            };
                        case "xml":
                            return value -> {
                                if (value instanceof String) {
                                    return new StringColumn((String) value);
                                } else if (value instanceof PgSQLXML) {
                                    PgSQLXML pgSQLXML = (PgSQLXML) value;
                                    try {
                                        return new StringColumn(pgSQLXML.getString());
                                    } catch (SQLException e) {
                                        e.printStackTrace();
                                    }
                                }
                                return null;
                            };
                    }
                }

                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    static class GenericLogicalType<T> extends org.apache.flink.table.types.logical.LogicalType {

        private List<Class> in;
        private List<Class> out;
        private T wrapper;
        private String desc;

        public GenericLogicalType(
                Boolean isNullable, String desc, List<Class> in, List<Class> out) {
            super(isNullable, org.apache.flink.table.types.logical.LogicalTypeRoot.RAW);
            this.desc = desc;
            TypeVariable<? extends Class<? extends GenericLogicalType>>[] parameters =
                    this.getClass().getTypeParameters();
            this.wrapper = (T) parameters[0];
            this.in = in;
            this.out = out;
        }

        @Override
        public org.apache.flink.table.types.logical.LogicalType copy(boolean isNullable) {
            return new GenericLogicalType<T>(isNullable, this.desc, this.in, this.out);
        }

        @Override
        public String asSerializableString() {
            return desc;
        }

        @Override
        public boolean supportsInputConversion(Class<?> clazz) {
            return in.contains(clazz);
        }

        @Override
        public boolean supportsOutputConversion(Class<?> clazz) {
            return out.contains(clazz);
        }

        @Override
        public Class<?> getDefaultConversion() {
            return String.class;
        }

        @Override
        public java.util.List<org.apache.flink.table.types.logical.LogicalType> getChildren() {
            return java.util.Collections.emptyList();
        }

        @Override
        public <R> R accept(org.apache.flink.table.types.logical.LogicalTypeVisitor<R> visitor) {
            return visitor.visit(this);
        }

        public String type() {
            return desc;
        }
    }

    static class DataTypeFactoryMock implements org.apache.flink.table.catalog.DataTypeFactory {

        public Optional<DataType> dataType = Optional.empty();

        @Override
        public DataType createDataType(AbstractDataType<?> abstractDataType) {
            if (abstractDataType instanceof DataType) {
                return (DataType) abstractDataType;
            } else if (abstractDataType instanceof UnresolvedDataType) {
                return ((UnresolvedDataType) abstractDataType).toDataType(this);
            }
            throw new IllegalStateException();
        }

        @Override
        public DataType createDataType(String name) {
            if (registered.containsKey(name)) {
                return new AtomicDataType(registered.get(name));
            }
            return TypeConversions.fromLogicalToDataType(LogicalTypeParser.parse(name));
        }

        @Override
        public DataType createDataType(UnresolvedIdentifier identifier) {
            return dataType.orElseThrow(() -> new ValidationException("No type found."));
        }

        @Override
        public <T> DataType createDataType(Class<T> clazz) {
            return DataTypeExtractor.extractFromType(this, clazz);
        }

        @Override
        public <T> DataType createDataType(TypeInformation<T> typeInformation) {
            return TypeInfoDataTypeConverter.toDataType(this, typeInformation);
        }

        @Override
        public <T> DataType createRawDataType(Class<T> clazz) {
            return dataType.orElseThrow(IllegalStateException::new);
        }

        @Override
        public <T> DataType createRawDataType(TypeInformation<T> typeInformation) {
            return dataType.orElseThrow(IllegalStateException::new);
        }
    }

    static class DataTypeFactory {

        public static DataType getDataType(String type) {
            return null;
        }
    }
}
