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

package com.dtstack.flinkx.connector.stream.util;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.datagen.DataGeneratorContainer;
import org.apache.flink.table.factories.datagen.RandomGeneratorVisitor;
import org.apache.flink.table.factories.datagen.SequenceGeneratorVisitor;
import org.apache.flink.table.types.DataType;

/**
 * @author chuixue
 * @create 2021-04-25 14:18
 * @description
 **/
public class DataGeneratorUtil {
    public static final String FIELDS = "fields";
    public static final String KIND = "kind";
    public static final String START = "start";
    public static final String END = "end";
    public static final String MIN = "min";
    public static final String MAX = "max";
    public static final String LENGTH = "length";

    public static final String SEQUENCE = "sequence";
    public static final String RANDOM = "random";

    public static DataGeneratorContainer createContainer(String name, DataType type, String kind, ReadableConfig options) {
        switch (kind) {
            case RANDOM:
                return type.getLogicalType().accept(new RandomGeneratorVisitor(name, options));
            case SEQUENCE:
                return type.getLogicalType().accept(new SequenceGeneratorVisitor(name, options));
            default:
                throw new ValidationException("Unsupported generator kind: " + kind);
        }
    }
}
