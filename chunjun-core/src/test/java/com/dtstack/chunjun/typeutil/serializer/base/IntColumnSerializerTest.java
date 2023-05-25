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
import com.dtstack.chunjun.element.column.IntColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.typeutil.SerializerTestBase;
import com.dtstack.chunjun.typeutil.serializer.DeeplyEqualsChecker;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.function.BiFunction;

public class IntColumnSerializerTest extends SerializerTestBase<AbstractBaseColumn> {

    @Override
    protected Tuple2<BiFunction<Object, Object, Boolean>, DeeplyEqualsChecker.CustomEqualityChecker>
            getCustomChecker() {
        return Tuple2.of(
                (o, o2) ->
                        (o instanceof IntColumn && o2 instanceof IntColumn)
                                || (o instanceof NullColumn && o2 instanceof NullColumn),
                new IntColumnChecker());
    }

    @Override
    protected TypeSerializer<AbstractBaseColumn> createSerializer() {
        return IntColumnSerializer.INSTANCE;
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
            new NullColumn(), new IntColumn(123), new IntColumn(212), new IntColumn(1234312),
        };
    }

    public static class IntColumnChecker implements DeeplyEqualsChecker.CustomEqualityChecker {

        @Override
        public boolean check(Object o1, Object o2, DeeplyEqualsChecker checker) {
            if (o1 instanceof IntColumn && o2 instanceof IntColumn) {
                return ((IntColumn) o1).asInt().compareTo(((IntColumn) o2).asInt()) == 0;
            } else {
                return o1 instanceof NullColumn && o2 instanceof NullColumn;
            }
        }
    }
}
