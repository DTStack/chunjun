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

package com.dtstack.chunjun.connector.hbase.converter;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.hbase.config.HBaseConfig;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.IntColumn;
import com.dtstack.chunjun.element.column.LongColumn;
import com.dtstack.chunjun.element.column.ShortColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.Mutation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class HBaseSyncConverterTest {

    /**
     * The number of milliseconds in a day.
     *
     * <p>This is the modulo 'mask' used when converting TIMESTAMP values to DATE and TIME values.
     */
    private static final long MILLIS_PER_DAY = 86400000L; // = 24 * 60 * 60 * 1000

    /** The local time zone. */
    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    @Before
    public void setUp() {}

    @Test
    public void testConstructorOfHBaseColumnConverter() throws Exception {
        Map<String, Object> confMap = Maps.newHashMap();
        List<FieldConfig> columnList = Lists.newArrayList();
        HBaseConfig conf = new HBaseConfig();
        ColumnRowData rowData = new ColumnRowData(RowKind.INSERT, 15);

        FieldConfig id = new FieldConfig();
        id.setName("stu:id");
        id.setType(TypeConfig.fromString("int"));
        rowData.addField(new IntColumn(1));

        FieldConfig decimal_val = new FieldConfig();
        decimal_val.setName("msg:decimal_val");
        decimal_val.setType(TypeConfig.fromString("decimal(38, 18)"));
        rowData.addField(new BigDecimalColumn(new BigDecimal("3.3")));

        FieldConfig float_val = new FieldConfig();
        float_val.setName("msg:float_val");
        float_val.setType(TypeConfig.fromString("float"));
        rowData.addField(new FloatColumn(3.33f));

        FieldConfig smallint_val = new FieldConfig();
        smallint_val.setName("msg:smallint_val");
        smallint_val.setType(TypeConfig.fromString("smallint"));
        rowData.addField(new ShortColumn((short) 3));

        FieldConfig bigint_val = new FieldConfig();
        bigint_val.setName("msg:bigint_val");
        bigint_val.setType(TypeConfig.fromString("bigint"));
        rowData.addField(new LongColumn(1));

        FieldConfig boolean_val = new FieldConfig();
        boolean_val.setName("msg:boolean_val");
        boolean_val.setType(TypeConfig.fromString("boolean"));
        rowData.addField(new BooleanColumn(false));

        FieldConfig tinyint_val = new FieldConfig();
        tinyint_val.setName("msg:tinyint_val");
        tinyint_val.setType(TypeConfig.fromString("tinyint"));
        rowData.addField(new ByteColumn((byte) 1));

        FieldConfig date_val = new FieldConfig();
        date_val.setName("msg:date_val");
        date_val.setType(TypeConfig.fromString("date"));
        rowData.addField(new SqlDateColumn(Date.valueOf("2022-08-26")));

        FieldConfig time_val = new FieldConfig();
        time_val.setName("msg:time_val");
        time_val.setType(TypeConfig.fromString("time"));
        rowData.addField(new TimeColumn(Time.valueOf("11:06:14")));

        FieldConfig timestamp_val = new FieldConfig();
        timestamp_val.setName("msg:timestamp_val");
        timestamp_val.setType(TypeConfig.fromString("timestamp(3)"));
        rowData.addField(new TimestampColumn(System.currentTimeMillis()));

        FieldConfig datetime_val = new FieldConfig();
        datetime_val.setName("msg:datetime_val");
        datetime_val.setType(TypeConfig.fromString("datetime"));
        rowData.addField(new TimestampColumn(System.currentTimeMillis()));

        FieldConfig bytes_val = new FieldConfig();
        bytes_val.setName("msg:bytes_val");
        bytes_val.setType(TypeConfig.fromString("bytes"));
        rowData.addField(new BytesColumn("test".getBytes(StandardCharsets.UTF_8)));

        FieldConfig varchar_val = new FieldConfig();
        varchar_val.setName("msg:varchar_val");
        varchar_val.setType(TypeConfig.fromString("varchar(255)"));
        rowData.addField(new StringColumn("test"));

        FieldConfig double_val = new FieldConfig();
        double_val.setName("msg:double_val");
        double_val.setType(TypeConfig.fromString("double"));
        rowData.addField(new DoubleColumn(3.33));

        FieldConfig val_1 = new FieldConfig();
        val_1.setName("val_1");
        val_1.setType(TypeConfig.fromString("string"));
        val_1.setValue("val_1");

        columnList.add(id);
        columnList.add(decimal_val);
        columnList.add(float_val);
        columnList.add(smallint_val);
        columnList.add(bigint_val);
        columnList.add(boolean_val);
        columnList.add(tinyint_val);
        columnList.add(date_val);
        columnList.add(time_val);
        columnList.add(timestamp_val);
        columnList.add(datetime_val);
        columnList.add(bytes_val);
        columnList.add(varchar_val);
        columnList.add(double_val);
        // columnList.add(val_1);

        conf.setHbaseConfig(confMap);
        conf.setColumn(columnList);
        conf.setEncoding(StandardCharsets.UTF_8.name());
        conf.setStartRowkey("start");
        conf.setEndRowkey("end");
        conf.setBinaryRowkey(true);
        conf.setTable("test_table");
        conf.setScanCacheSize(1000);

        conf.setNullMode("TEST_NULL_MODE");
        conf.setNullStringLiteral("N/A");
        conf.setWalFlag(true);
        conf.setWriteBufferSize(1000);
        conf.setRowkeyExpress("$(stu:id)");
        conf.setVersionColumnIndex(1);
        conf.setVersionColumnValue("VERSION");

        RowType rowType = TableUtil.createRowType(conf.getColumn(), HBaseRawTypeMapper.INSTANCE);
        HBaseSyncConverter converter = new HBaseSyncConverter(conf, rowType);

        Assert.assertEquals("stu:id", converter.getCommonConfig().getColumn().get(0).getName());

        Mutation toExternal = converter.toExternal(rowData, null);
        Assert.assertFalse(toExternal.getFamilyCellMap().isEmpty());
    }

    @Test
    public void testConstructorOfHBaseFlatRowConverter() throws Exception {
        Map<String, Object> confMap = Maps.newHashMap();
        List<FieldConfig> columnList = Lists.newArrayList();
        HBaseConfig conf = new HBaseConfig();
        GenericRowData rowData = new GenericRowData(RowKind.INSERT, 12);

        FieldConfig id = new FieldConfig();
        id.setName("stu:id");
        id.setType(TypeConfig.fromString("int"));
        rowData.setField(0, 1);

        FieldConfig decimal_val = new FieldConfig();
        decimal_val.setName("msg:decimal_val");
        decimal_val.setType(TypeConfig.fromString("decimal(38, 18)"));
        rowData.setField(1, DecimalData.fromBigDecimal(new BigDecimal("3.3"), 38, 18));

        FieldConfig float_val = new FieldConfig();
        float_val.setName("msg:float_val");
        float_val.setType(TypeConfig.fromString("float"));
        rowData.setField(2, 3.3f);

        FieldConfig bigint_val = new FieldConfig();
        bigint_val.setName("msg:bigint_val");
        bigint_val.setType(TypeConfig.fromString("bigint"));
        rowData.setField(3, 1L);

        FieldConfig boolean_val = new FieldConfig();
        boolean_val.setName("msg:boolean_val");
        boolean_val.setType(TypeConfig.fromString("boolean"));
        rowData.setField(4, false);

        FieldConfig date_val = new FieldConfig();
        date_val.setName("msg:date_val");
        date_val.setType(TypeConfig.fromString("date"));
        rowData.setField(5, HBaseSyncConverterTest.dateToInternal(Date.valueOf("2022-08-26")));

        FieldConfig time_val = new FieldConfig();
        time_val.setName("msg:time_val");
        time_val.setType(TypeConfig.fromString("time"));
        rowData.setField(6, HBaseSyncConverterTest.timeToInternal(Time.valueOf("11:06:14")));

        FieldConfig timestamp_val = new FieldConfig();
        timestamp_val.setName("msg:timestamp_val");
        timestamp_val.setType(TypeConfig.fromString("timestamp(3)"));
        rowData.setField(7, TimestampData.fromTimestamp(Timestamp.valueOf("2022-08-24 11:08:09")));

        FieldConfig datetime_val = new FieldConfig();
        datetime_val.setName("msg:datetime_val");
        datetime_val.setType(TypeConfig.fromString("datetime"));
        rowData.setField(8, TimestampData.fromTimestamp(Timestamp.valueOf("2022-08-24 11:08:09")));

        FieldConfig bytes_val = new FieldConfig();
        bytes_val.setName("msg:bytes_val");
        bytes_val.setType(TypeConfig.fromString("bytes"));
        rowData.setField(9, "test".getBytes(StandardCharsets.UTF_8));

        FieldConfig varchar_val = new FieldConfig();
        varchar_val.setName("msg:varchar_val");
        varchar_val.setType(TypeConfig.fromString("varchar(255)"));
        rowData.setField(10, StringData.fromString("test"));

        FieldConfig double_val = new FieldConfig();
        double_val.setName("msg:double_val");
        double_val.setType(TypeConfig.fromString("double"));
        rowData.setField(11, 3.3);

        columnList.add(id);
        columnList.add(decimal_val);
        columnList.add(float_val);
        columnList.add(bigint_val);
        columnList.add(boolean_val);
        columnList.add(date_val);
        columnList.add(time_val);
        columnList.add(timestamp_val);
        columnList.add(datetime_val);
        columnList.add(bytes_val);
        columnList.add(varchar_val);
        columnList.add(double_val);

        conf.setHbaseConfig(confMap);
        conf.setColumn(columnList);
        conf.setEncoding(StandardCharsets.UTF_8.name());
        conf.setStartRowkey("start");
        conf.setEndRowkey("end");
        conf.setBinaryRowkey(true);
        conf.setTable("test_table");
        conf.setScanCacheSize(1000);

        conf.setNullMode("TEST_NULL_MODE");
        conf.setNullStringLiteral("N/A");
        conf.setWalFlag(true);
        conf.setWriteBufferSize(1000);
        conf.setRowkeyExpress("$(stu:id)");
        conf.setVersionColumnIndex(null);
        conf.setVersionColumnValue("2022-08-24 11:08:09");

        RowType rowType = TableUtil.createRowType(conf.getColumn(), HBaseRawTypeMapper.INSTANCE);
        HBaseFlatRowConverter converter = new HBaseFlatRowConverter(conf, rowType);

        Assert.assertEquals("stu:id", converter.getCommonConfig().getColumn().get(0).getName());

        Mutation toExternal = converter.toExternal(rowData, null);
        Assert.assertFalse(toExternal.getFamilyCellMap().isEmpty());
    }

    /**
     * Converts the Java type used for UDF parameters of SQL DATE type ({@link java.sql.Date}) to
     * internal representation (int).
     */
    public static int dateToInternal(java.sql.Date date) {
        long ts = date.getTime() + LOCAL_TZ.getOffset(date.getTime());
        return (int) (ts / MILLIS_PER_DAY);
    }

    /**
     * Converts the Java type used for UDF parameters of SQL TIME type ({@link java.sql.Time}) to
     * internal representation (int).
     */
    public static int timeToInternal(java.sql.Time time) {
        long ts = time.getTime() + LOCAL_TZ.getOffset(time.getTime());
        return (int) (ts % MILLIS_PER_DAY);
    }
}
