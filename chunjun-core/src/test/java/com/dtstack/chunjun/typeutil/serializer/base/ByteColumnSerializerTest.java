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

package com.dtstack.chunjun.typeutil.serializer.base;

import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.typeutil.SerializerTestBase;
import com.dtstack.chunjun.typeutil.serializer.DeeplyEqualsChecker;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.function.BiFunction;

public class ByteColumnSerializerTest extends SerializerTestBase<AbstractBaseColumn> {

    @Override
    protected Tuple2<BiFunction<Object, Object, Boolean>, DeeplyEqualsChecker.CustomEqualityChecker>
            getCustomChecker() {
        return Tuple2.of(
                (o, o2) ->
                        (o instanceof ByteColumn && o2 instanceof ByteColumn)
                                || (o instanceof NullColumn && o2 instanceof NullColumn),
                new ByteColumnChecker());
    }

    @Override
    protected TypeSerializer<AbstractBaseColumn> createSerializer() {
        return ByteColumnSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<AbstractBaseColumn> getTypeClass() {
        return AbstractBaseColumn.class;
    }

    @Override
    protected AbstractBaseColumn[] getTestData() {
        return new AbstractBaseColumn[] {
            new NullColumn(), new ByteColumn((byte) 0), new ByteColumn((byte) 12)
        };
    }

    public static class ByteColumnChecker implements DeeplyEqualsChecker.CustomEqualityChecker {

        @Override
        public boolean check(Object o1, Object o2, DeeplyEqualsChecker checker) {
            if (o1 instanceof ByteColumn && o2 instanceof ByteColumn) {
                return ((ByteColumn) o1).getData() == ((ByteColumn) o2).getData();
            } else {
                return o1 instanceof NullColumn && o2 instanceof NullColumn;
            }
        }
    }
}
