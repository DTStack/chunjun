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
import com.dtstack.chunjun.element.column.MapColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.typeutil.SerializerTestBase;
import com.dtstack.chunjun.typeutil.serializer.DeeplyEqualsChecker;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class MapColumnSerializerTest extends SerializerTestBase<AbstractBaseColumn> {
    @Override
    protected Tuple2<BiFunction<Object, Object, Boolean>, DeeplyEqualsChecker.CustomEqualityChecker>
            getCustomChecker() {
        return Tuple2.of(
                (o, o2) ->
                        (o instanceof MapColumn && o2 instanceof MapColumn)
                                || (o instanceof NullColumn && o2 instanceof NullColumn),
                new MapColumnChecker());
    }

    @Override
    protected TypeSerializer<AbstractBaseColumn> createSerializer() {
        return MapColumnSerializer.INSTANCE;
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
        Map<String, Object> map = new HashMap<>();
        List<Map<String, Object>> arrayListData = new ArrayList<>();
        Map<String, Object> mapData = new HashMap<>();
        mapData.put("1", 1);
        mapData.put("2", 2);
        arrayListData.add(mapData);
        map.put("arrayListData", arrayListData);
        map.put("3", 3);
        map.put("4", "4");
        return new AbstractBaseColumn[] {new MapColumn(map)};
    }

    public static class MapColumnChecker implements DeeplyEqualsChecker.CustomEqualityChecker {

        @Override
        public boolean check(Object o1, Object o2, DeeplyEqualsChecker checker) {
            if (o1 instanceof MapColumn && o2 instanceof MapColumn) {
                Map<String, Object> want = (Map<String, Object>) ((MapColumn) o1).getData();
                Map<String, Object> that = (Map<String, Object>) ((MapColumn) o2).getData();
                if (want.size() != that.size()) {
                    return false;
                }
                return JsonUtil.toJson(want).equals(JsonUtil.toJson(that));
            } else {
                return o1 instanceof NullColumn && o2 instanceof NullColumn;
            }
        }
    }
}
