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

package com.dtstack.chunjun.source;

import com.dtstack.chunjun.source.format.MockInputFormat;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

public class MockTypeInfo<T> extends TypeInformation<T> {

    private boolean success;

    public MockTypeInfo(boolean success) {
        this.success = success;
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 0;
    }

    @Override
    public int getTotalFields() {
        return 0;
    }

    @Override
    public Class<T> getTypeClass() {
        return null;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        return (TypeSerializer<T>) new MockTypeSerializer(success);
    }

    @Override
    public String toString() {
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean canEqual(Object obj) {
        return false;
    }

    private static class MockTypeSerializer extends TypeSerializer<RowData> {

        private boolean success;

        public MockTypeSerializer(boolean success) {
            this.success = success;
        }

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public TypeSerializer<RowData> duplicate() {
            return null;
        }

        @Override
        public RowData createInstance() {
            if (success) {
                return MockInputFormat.SUCCESS_DATA;
            } else {
                return MockInputFormat.ERROR_DATA;
            }
        }

        @Override
        public RowData copy(RowData from) {
            return null;
        }

        @Override
        public RowData copy(RowData from, RowData reuse) {
            return null;
        }

        @Override
        public int getLength() {
            return 0;
        }

        @Override
        public void serialize(RowData record, DataOutputView target) throws IOException {}

        @Override
        public RowData deserialize(DataInputView source) throws IOException {
            return null;
        }

        @Override
        public RowData deserialize(RowData reuse, DataInputView source) throws IOException {
            return null;
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {}

        @Override
        public boolean equals(Object obj) {
            return false;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public TypeSerializerSnapshot<RowData> snapshotConfiguration() {
            return null;
        }
    }
}
