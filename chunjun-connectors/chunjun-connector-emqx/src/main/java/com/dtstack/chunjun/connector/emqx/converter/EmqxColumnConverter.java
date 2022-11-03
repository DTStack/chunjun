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

import com.dtstack.chunjun.connector.emqx.conf.EmqxConf;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.decoder.IDecode;
import com.dtstack.chunjun.decoder.JsonDecoder;
import com.dtstack.chunjun.decoder.TextDecoder;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.MapColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.util.MapUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.dtstack.chunjun.connector.emqx.options.EmqxOptions.DEFAULT_CODEC;

/**
 * @author chuixue
 * @create 2021-06-02 09:34
 * @description
 */
public class EmqxColumnConverter
        extends AbstractRowConverter<String, Object, MqttMessage, LogicalType> {

    /** emqx msg decode */
    private IDecode decode;
    /** json Decoder */
    private JsonDecoder jsonDecoder = new JsonDecoder();
    /** emqx Conf */
    private final EmqxConf emqxConf;

    public EmqxColumnConverter(EmqxConf emqxConf) {
        this.emqxConf = emqxConf;

        if (DEFAULT_CODEC.defaultValue().equals(emqxConf.getCodec())) {
            this.decode = new JsonDecoder();
        } else {
            this.decode = new TextDecoder();
        }
    }

    @Override
    public RowData toInternal(String input) {
        ColumnRowData row = new ColumnRowData(1);
        row.addField(new MapColumn(this.decode.decode(input)));
        return row;
    }

    @Override
    public MqttMessage toExternal(RowData rowData, MqttMessage output) throws Exception {
        Map<String, Object> map;
        int arity = rowData.getArity();
        ColumnRowData row = (ColumnRowData) rowData;

        if (emqxConf.getTableFields() != null
                && emqxConf.getTableFields().size() >= arity
                && !(row.getField(0) instanceof MapColumn)) {
            map = new LinkedHashMap<>((arity << 2) / 3);
            for (int i = 0; i < arity; i++) {
                Object obj = row.getField(i);
                Object value;
                if (obj instanceof TimestampColumn) {
                    value = ((TimestampColumn) obj).asTimestampStr();
                } else {
                    value = org.apache.flink.util.StringUtils.arrayAwareToString(obj);
                }
                map.put(emqxConf.getTableFields().get(i), value);
            }
        } else if (arity == 1) {
            Object obj = row.getField(0);
            if (obj instanceof MapColumn) {
                map = (Map<String, Object>) ((MapColumn) obj).getData();
            } else if (obj instanceof StringColumn) {
                map = jsonDecoder.decode(obj.toString());
            } else {
                map = Collections.singletonMap("message", row.getString());
            }
        } else {
            map = Collections.singletonMap("message", row.getString());
        }

        output.setPayload(MapUtil.writeValueAsBytes(map));
        return output;
    }
}
