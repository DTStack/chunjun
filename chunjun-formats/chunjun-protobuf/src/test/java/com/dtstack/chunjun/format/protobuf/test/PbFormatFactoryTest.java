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

package com.dtstack.chunjun.format.protobuf.test;

import com.dtstack.chunjun.format.protobuf.PbFormatFactory;
import com.dtstack.chunjun.format.protobuf.util.FormatCheckUtil;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static com.dtstack.chunjun.format.protobuf.PbFormatOptions.MESSAGE_CLASS_NAME;

/** @author liuliu 2022/5/4 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({PbFormatFactory.class})
public class PbFormatFactoryTest {

    DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    DeserializationSchema<RowData> runtimeDecoder;
    SerializationSchema<RowData> runtimeEncoder;

    @Before
    public void init() throws Exception {
        DynamicTableFactory.Context context = PowerMockito.mock(DynamicTableFactory.Context.class);
        ReadableConfig readableConfig = PowerMockito.mock(ReadableConfig.class);
        PowerMockito.when(readableConfig.get(MESSAGE_CLASS_NAME))
                .thenReturn(
                        "com.dtstack.chunjun.format.protobuf.test.MessageTestOuterClass$MessageTest");

        PbFormatFactory pbFormatFactory = PowerMockito.mock(PbFormatFactory.class);
        PowerMockito.when(pbFormatFactory, "createDecodingFormat", context, readableConfig)
                .thenCallRealMethod();
        PowerMockito.when(pbFormatFactory, "createEncodingFormat", context, readableConfig)
                .thenCallRealMethod();
        decodingFormat = pbFormatFactory.createDecodingFormat(context, readableConfig);
        encodingFormat = pbFormatFactory.createEncodingFormat(context, readableConfig);

        DynamicTableSource.Context sourceContext =
                PowerMockito.mock(DynamicTableSource.Context.class);
        DynamicTableSink.Context sinkContext = PowerMockito.mock(DynamicTableSink.Context.class);

        FormatCheckUtil formatCheckUtil =
                new FormatCheckUtil(
                        null,
                        "com.dtstack.chunjun.format.protobuf.test.MessageTestOuterClass$MessageTest");
        RowType messageLogicalType =
                formatCheckUtil.createMessageLogicalType(
                        "com.dtstack.chunjun.format.protobuf.test.MessageTestOuterClass$MessageTest");
        DataType physicalDataType = PowerMockito.mock(DataType.class);
        PowerMockito.when(physicalDataType.getLogicalType()).thenReturn(messageLogicalType);
        PowerMockito.when(sourceContext.createTypeInformation(physicalDataType)).thenReturn(null);
        PowerMockito.when(sinkContext.createTypeInformation(physicalDataType)).thenReturn(null);

        runtimeDecoder = decodingFormat.createRuntimeDecoder(sourceContext, physicalDataType);

        runtimeEncoder = encodingFormat.createRuntimeEncoder(sinkContext, physicalDataType);
    }

    @Test
    public void serializeTest() throws Exception {

        MessageTestOuterClass.MessageTest messageGroup = getMessageGroup();
        // serialize
        runtimeDecoder.open(null);
        RowData deserialize = runtimeDecoder.deserialize(messageGroup.toByteArray());
        assert deserialize.getMap(0).keyArray().getString(0).toString().equals("group");
        assert deserialize.getArray(1).getRow(0, 0).getRow(1, 0).getRow(1, 0).getInt(0) == 3;
    }

    @Test
    public void deserializeTest() throws Exception {

        MessageTestOuterClass.MessageTest messageGroup = getMessageGroup();
        runtimeDecoder.open(null);
        RowData rowData = runtimeDecoder.deserialize(messageGroup.toByteArray());

        runtimeEncoder.open(null);
        assert messageGroup.equals(
                MessageTestOuterClass.MessageTest.parseFrom(runtimeEncoder.serialize(rowData)));
    }

    public static MessageTestOuterClass.MessageTest getMessageGroup() {
        MessageTestOuterClass.MessageTest.Builder builder =
                MessageTestOuterClass.MessageTest.newBuilder();
        builder.putGroupInfo("group", "test");
        builder.addMessages(0, getMessageItem());
        return builder.build();
    }

    public static MessageTestOuterClass.MessageItem getMessageItem() {
        MessageTestOuterClass.MessageItem.Builder builder =
                MessageTestOuterClass.MessageItem.newBuilder();

        builder.setTagName("tag");
        builder.setTagValue(getVariant());
        builder.putExValues("2", "3");

        return builder.build();
    }

    public static MessageTestOuterClass.Variant getVariant() {
        MessageTestOuterClass.Variant.Builder builder = MessageTestOuterClass.Variant.newBuilder();
        builder.setBoolx(true);
        builder.setValueInt32(1);
        builder.setBooly(false);

        return builder.build();
    }
}
