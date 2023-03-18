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
package com.dtstack.chunjun.connector.kafka.deserializer;

import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class DtKafkaDeserializer<T> implements Deserializer<byte[]> {
    protected ObjectMapper objectMapper = new ObjectMapper();
    private Deserializer<T> deserializer;

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        String deserializerClassName;
        if (isKey) {
            deserializerClassName = (String) configs.get("dt.key.deserializer");
        } else {
            deserializerClassName = (String) configs.get("dt.value.deserializer");
        }
        try {
            this.deserializer =
                    (Deserializer<T>) Class.forName(deserializerClassName).newInstance();
        } catch (Exception e) {
            throw new ChunJunRuntimeException("Can't create instance: " + deserializerClassName, e);
        }
        this.deserializer.configure(configs, isKey);
    }

    @Override
    public byte[] deserialize(String topic, Headers headers, byte[] data) {
        return toBytes(deserializer.deserialize(topic, headers, data));
    }

    @Override
    public byte[] deserialize(String topic, byte[] data) {
        return toBytes(deserializer.deserialize(topic, data));
    }

    /**
     * T value to byte[]
     *
     * @param value
     * @return
     */
    private byte[] toBytes(T value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        } else {
            try {
                return this.objectMapper
                        .readValue(this.objectMapper.writeValueAsBytes(value), String.class)
                        .getBytes(StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new SerializationException("Can't deserialize data", e);
            }
        }
    }

    @Override
    public void close() {
        this.deserializer.close();
    }
}
