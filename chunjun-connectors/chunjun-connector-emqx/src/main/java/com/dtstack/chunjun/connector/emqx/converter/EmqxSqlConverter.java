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

package com.dtstack.chunjun.connector.emqx.converter;

import com.dtstack.chunjun.converter.AbstractRowConverter;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.nio.charset.StandardCharsets;

public class EmqxSqlConverter
        extends AbstractRowConverter<String, Object, MqttMessage, LogicalType> {

    private static final long serialVersionUID = -7956302669069858454L;

    /** DeserializationSchema Instead of source */
    private DeserializationSchema<RowData> valueDeserialization;
    /** SerializationSchema Instead of sink */
    private SerializationSchema<RowData> valueSerialization;

    public EmqxSqlConverter(DeserializationSchema<RowData> valueDeserialization) {
        this.valueDeserialization = valueDeserialization;
    }

    public EmqxSqlConverter(SerializationSchema<RowData> valueSerialization) {
        this.valueSerialization = valueSerialization;
    }

    @Override
    public RowData toInternal(String input) throws Exception {
        return valueDeserialization.deserialize(input.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public MqttMessage toExternal(RowData rowData, MqttMessage output) {
        try {
            valueSerialization.open(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        final byte[] valueSerialized = valueSerialization.serialize(rowData);
        output.setPayload(valueSerialized);
        return output;
    }
}
