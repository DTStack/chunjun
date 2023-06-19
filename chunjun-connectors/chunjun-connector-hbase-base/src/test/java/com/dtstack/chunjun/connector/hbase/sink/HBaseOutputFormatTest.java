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

package com.dtstack.chunjun.connector.hbase.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.hbase.config.HBaseConfig;
import com.dtstack.chunjun.connector.hbase.converter.HBaseRawTypeMapper;
import com.dtstack.chunjun.connector.hbase.converter.HBaseSyncConverter;
import com.dtstack.chunjun.connector.hbase.util.HBaseHelperTest;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.reflect.Whitebox.setInternalState;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    HBaseHelperTest.class,
    HBaseConfiguration.class,
    ConnectionFactory.class,
})
@PowerMockIgnore("javax.management.*")
public class HBaseOutputFormatTest {

    private HBaseOutputFormat format;

    private Connection connection;

    private BufferedMutator bufferedMutator;

    private Table table;

    private AbstractRowConverter converter;

    private ColumnRowData rowData;

    private Admin admin;

    @Before
    public void setUp() throws IOException {
        PowerMockito.mockStatic(HBaseHelperTest.class);
        PowerMockito.mockStatic(HBaseConfiguration.class);
        PowerMockito.mockStatic(ConnectionFactory.class);

        org.apache.hadoop.conf.Configuration mockConfig =
                mock(org.apache.hadoop.conf.Configuration.class);
        when(HBaseConfiguration.create())
                .thenAnswer((Answer<org.apache.hadoop.conf.Configuration>) answer -> mockConfig);
        when(ConnectionFactory.createConnection(mockConfig))
                .thenAnswer((Answer<Connection>) answer -> connection);

        connection = mock(Connection.class);
        bufferedMutator = mock(BufferedMutator.class);
        table = mock(Table.class);
        admin = mock(Admin.class);

        Map<String, Object> confMap = Maps.newHashMap();
        List<FieldConfig> columnList = Lists.newArrayList();
        HBaseConfig conf = new HBaseConfig();
        rowData = new ColumnRowData(RowKind.INSERT, 14);

        FieldConfig id = new FieldConfig();
        id.setName("stu:id");
        id.setType(TypeConfig.fromString("int"));
        rowData.addField(new BigDecimalColumn(1));

        FieldConfig decimal_val = new FieldConfig();
        decimal_val.setName("msg:decimal_val");
        decimal_val.setType(TypeConfig.fromString("decimal(38, 18)"));
        rowData.addField(new BigDecimalColumn(3));

        FieldConfig float_val = new FieldConfig();
        float_val.setName("msg:float_val");
        float_val.setType(TypeConfig.fromString("float"));
        rowData.addField(new FloatColumn(3.33f));

        FieldConfig smallint_val = new FieldConfig();
        smallint_val.setName("msg:smallint_val");
        smallint_val.setType(TypeConfig.fromString("smallint"));
        rowData.addField(new BigDecimalColumn(3));

        FieldConfig bigint_val = new FieldConfig();
        bigint_val.setName("msg:bigint_val");
        bigint_val.setType(TypeConfig.fromString("bigint"));
        rowData.addField(new BigDecimalColumn(1));

        FieldConfig boolean_val = new FieldConfig();
        boolean_val.setName("msg:boolean_val");
        boolean_val.setType(TypeConfig.fromString("boolean"));
        rowData.addField(new BooleanColumn(false));

        FieldConfig tinyint_val = new FieldConfig();
        tinyint_val.setName("msg:tinyint_val");
        tinyint_val.setType(TypeConfig.fromString("tinyint"));
        rowData.addField(new BigDecimalColumn(1));

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
        converter = new HBaseSyncConverter(conf, rowType);

        HBaseOutputFormatBuilder formatBuilder = new HBaseOutputFormatBuilder();

        formatBuilder.setTableName("hbase_test");

        Map<String, Object> hbaseConfig = Maps.newHashMap();
        formatBuilder.setHbaseConfig(hbaseConfig);

        HBaseConfig hBaseConfig = new HBaseConfig();
        formatBuilder.setConfig(hBaseConfig);

        formatBuilder.setWriteBufferSize(null);
        formatBuilder.setWriteBufferSize(1000L);

        formatBuilder.checkFormat();
        format = (HBaseOutputFormat) formatBuilder.finish();
        format.setRowConverter(converter);
        setInternalState(format, "connection", connection);
        setInternalState(format, "table", table);
        setInternalState(format, "bufferedMutator", bufferedMutator);
    }

    @Test
    public void testBuilder() {
        Assert.assertEquals("hbase_test", format.getTableName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOpenConnectionWithNullConfigThenThrowsException() {
        HBaseOutputFormat outputFormat = new HBaseOutputFormat();
        outputFormat.configure(new Configuration());
        outputFormat.openConnection();
    }

    @Test
    public void testWriteSingleRecordInternal() throws WriteRecordException, IOException {
        doNothing().when(bufferedMutator).mutate(any(Mutation.class));
        doNothing().when(bufferedMutator).flush();
        format.writeSingleRecordInternal(rowData);
    }

    @Test(expected = WriteRecordException.class)
    public void testWriteSingleRecordInternalThenThrowWriteRecordException()
            throws WriteRecordException, IOException {
        doNothing().when(bufferedMutator).mutate(any(Mutation.class));
        doNothing().when(bufferedMutator).flush();
        ColumnRowData data = new ColumnRowData(RowKind.INSERT, 1);
        data.addField(new BigDecimalColumn(1));
        format.writeSingleRecordInternal(data);
    }

    @Test
    public void testWriteMultipleRecordsInternal() throws Exception {
        doNothing().when(table).batch(anyList(), any());
        List<RowData> rows = new ArrayList<>();
        rows.add(rowData);
        setInternalState(format, "rows", rows);

        format.writeMultipleRecordsInternal();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOpenInternalThenThrowsIOException() throws IOException {
        final String k1 = "where?";
        final String v1 = "I'm on a boat";
        final String k2 = "when?";
        final String v2 = "midnight";
        final String k3 = "why?";
        final String v3 = "what do you think?";
        final String k4 = "which way?";
        final String v4 = "south, always south...";

        Map<String, Object> confMap = Maps.newHashMap();
        confMap.put(k1, v1);
        confMap.put(k2, v2);
        confMap.put(k3, v3);
        confMap.put(k4, v4);

        setInternalState(format, "hbaseConfig", confMap);
        when(connection.getAdmin()).thenReturn(admin);
        when(admin.tableExists(any())).thenReturn(false);

        format.openInternal(1, 1);
    }

    @Test
    public void testCloseInternal() throws IOException {
        format.closeInternal();
    }
}
