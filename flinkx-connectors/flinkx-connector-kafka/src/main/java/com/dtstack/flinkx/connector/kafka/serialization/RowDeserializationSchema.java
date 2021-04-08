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
package com.dtstack.flinkx.connector.kafka.serialization;

import com.dtstack.flinkx.decoder.DecodeEnum;
import com.dtstack.flinkx.decoder.IDecode;
import com.dtstack.flinkx.decoder.JsonDecoder;
import com.dtstack.flinkx.decoder.TextDecoder;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import java.nio.charset.StandardCharsets;

/**
 * Date: 2021/03/04
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class RowDeserializationSchema extends AbstractDeserializationSchema<Row> {
    private static final long serialVersionUID = 1L;
    private String codec;
    private transient IDecode decode;

    public RowDeserializationSchema(String codec, TypeInformation<Row> typeInfo) {
        super(typeInfo);
        this.codec = codec;
    }

    @Override
    public Row deserialize(byte[] message) {
        if(decode == null){
            if (DecodeEnum.JSON.getName().equalsIgnoreCase(codec)) {
                decode = new JsonDecoder();
            } else {
                decode = new TextDecoder();
            }
        }
        return Row.of(this.decode.decode(new String(message, StandardCharsets.UTF_8)));
    }
}
