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

package com.dtstack.chunjun.typeutil.serializer;

import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.typeutil.serializer.base.BooleanColumnSerializer;
import com.dtstack.chunjun.typeutil.serializer.base.ByteColumnSerializer;
import com.dtstack.chunjun.typeutil.serializer.base.BytesColumnSerializer;
import com.dtstack.chunjun.typeutil.serializer.base.DecimalColumnSerializer;
import com.dtstack.chunjun.typeutil.serializer.base.DoubleColumnSerializer;
import com.dtstack.chunjun.typeutil.serializer.base.FloatColumnSerializer;
import com.dtstack.chunjun.typeutil.serializer.base.IntColumnSerializer;
import com.dtstack.chunjun.typeutil.serializer.base.LongColumnSerializer;
import com.dtstack.chunjun.typeutil.serializer.base.MapColumnSerializer;
import com.dtstack.chunjun.typeutil.serializer.base.NullColumnSerializer;
import com.dtstack.chunjun.typeutil.serializer.base.ShortColumnSerializer;
import com.dtstack.chunjun.typeutil.serializer.base.SqlDateColumnSerializer;
import com.dtstack.chunjun.typeutil.serializer.base.StringColumnSerializer;
import com.dtstack.chunjun.typeutil.serializer.base.TimeColumnSerializer;
import com.dtstack.chunjun.typeutil.serializer.base.TimestampColumnSerializer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.types.logical.LogicalType;

public class AbstractColumnSerializerUtil {

    /**
     * Creates a TypeSerializer for internal data structures of the given LogicalType and
     * descriptor.
     */
    public static TypeSerializer<AbstractBaseColumn> getTypeSerializer(
            LogicalType logicalType, String descriptor) {
        switch (logicalType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case STRUCTURED_TYPE:
                return new StringColumnSerializer(descriptor);
            case BOOLEAN:
                return BooleanColumnSerializer.INSTANCE;
            case BINARY:
            case VARBINARY:
                return BytesColumnSerializer.INSTANCE;
            case DECIMAL:
                return DecimalColumnSerializer.INSTANCE;
            case TINYINT:
                return ByteColumnSerializer.INSTANCE;
            case SMALLINT:
                return ShortColumnSerializer.INSTANCE;
            case INTERVAL_YEAR_MONTH:
            case INTEGER:
                return IntColumnSerializer.INSTANCE;
            case DATE:
                return SqlDateColumnSerializer.INSTANCE;
            case TIME_WITHOUT_TIME_ZONE:
                return TimeColumnSerializer.INSTANCE;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return LongColumnSerializer.INSTANCE;
            case FLOAT:
                return FloatColumnSerializer.INSTANCE;
            case DOUBLE:
                return DoubleColumnSerializer.INSTANCE;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampColumnSerializer.INSTANCE;
            case MAP:
                return MapColumnSerializer.INSTANCE;
            case NULL:
                return NullColumnSerializer.INSTANCE;
            case TIMESTAMP_WITH_TIME_ZONE:
            case ARRAY:
            case MULTISET:
            case ROW:
            case DISTINCT_TYPE:
            case RAW:
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type '" + logicalType + "' to get internal serializer");
        }
    }
}
