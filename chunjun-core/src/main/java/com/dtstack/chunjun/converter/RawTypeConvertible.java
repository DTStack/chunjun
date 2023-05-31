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

/**
 * The class implement this will be convert Raw Type to Flink Type. Implementations are
 * SourceFactory、SinkFactory、InputFormat、OutputFormat. When Flink running, Input/OutputFormat
 * convert raw type. When Flink setup, Source/SinkFactory convert raw type. When to convert depends
 * on the specific connector.
 */
public interface RawTypeConvertible {

    /**
     * Some connector (e.g. Kafka) that don't support to set raw type. Throw a {@link
     * UnsupportedOperationException} with this message.
     */
    String NO_SUPPORT_MSG = " connector don't support to set raw type.";

    /** @return Connector RawTypeConverter for parse data type string. */
    RawTypeMapper getRawTypeMapper();
}
