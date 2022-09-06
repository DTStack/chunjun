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

package com.dtstack.chunjun.connector.hbase;

import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

/** A utility class to process data exchange with HBase type system. */
public class HBaseTypeUtils {

    public static final int MIN_TIMESTAMP_PRECISION = 0;
    public static final int MAX_TIMESTAMP_PRECISION = 3;
    public static final int MIN_TIME_PRECISION = 0;
    public static final int MAX_TIME_PRECISION = 3;

    /** Checks whether the given {@link LogicalType} is supported in HBase connector. */
    public static boolean isUnsupportedType(LogicalType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
            case BIGINT:
            case INTERVAL_DAY_TIME:
            case FLOAT:
            case DOUBLE:
                return false;
            case TIME_WITHOUT_TIME_ZONE:
                final int timePrecision = getPrecision(type);
                if (timePrecision < MIN_TIME_PRECISION || timePrecision > MAX_TIME_PRECISION) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "The precision %s of TIME type is out of the range [%s, %s] supported by "
                                            + "HBase connector",
                                    timePrecision, MIN_TIME_PRECISION, MAX_TIME_PRECISION));
                }
                return false;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(type);
                if (timestampPrecision < MIN_TIMESTAMP_PRECISION
                        || timestampPrecision > MAX_TIMESTAMP_PRECISION) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "The precision %s of TIMESTAMP type is out of the range [%s, %s] supported by "
                                            + "HBase connector",
                                    timestampPrecision,
                                    MIN_TIMESTAMP_PRECISION,
                                    MAX_TIMESTAMP_PRECISION));
                }
                return false;
            case TIMESTAMP_WITH_TIME_ZONE:
            case ARRAY:
            case MULTISET:
            case MAP:
            case ROW:
            case STRUCTURED_TYPE:
            case DISTINCT_TYPE:
            case RAW:
            case NULL:
            case SYMBOL:
            case UNRESOLVED:
                return true;
            default:
                throw new IllegalArgumentException();
        }
    }
}
