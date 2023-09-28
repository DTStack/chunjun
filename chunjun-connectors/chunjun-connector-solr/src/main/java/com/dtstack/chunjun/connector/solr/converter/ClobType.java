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

package com.dtstack.chunjun.connector.solr.converter;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;

import java.io.Reader;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class ClobType extends LogicalType {

    private static final Set<String> INPUT_CONVERSION =
            conversionSet(String.class.getName(), StringData.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = String.class;

    private static final Set<String> OUTPUT_CONVERSION = conversionSet(Reader.class.getName());

    public ClobType(boolean isNullable, LogicalTypeRoot typeRoot) {
        super(isNullable, typeRoot);
    }

    @Override
    public String asSerializableString() {
        return "Saphana-Clob";
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return INPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public Class<?> getDefaultConversion() {
        return DEFAULT_CONVERSION;
    }

    @Override
    public List<LogicalType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new ClobType(isNullable, getTypeRoot());
    }
}
