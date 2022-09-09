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

package com.dtstack.chunjun.connector.starrocks.source.be;

import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;

import java.util.HashMap;
import java.util.List;

public interface StarRocksToJavaTrans {
    // StarRocks
    String DATA_TYPE_STARROCKS_DATE = "DATE";
    String DATA_TYPE_STARROCKS_DATETIME = "DATETIME";

    String DATA_TYPE_STARROCKS_CHAR = "CHAR";
    String DATA_TYPE_STARROCKS_VARCHAR = "VARCHAR";

    String DATA_TYPE_STARROCKS_BOOLEAN = "BOOLEAN";

    String DATA_TYPE_STARROCKS_TINYINT = "TINYINT";
    String DATA_TYPE_STARROCKS_SMALLINT = "SMALLINT";
    String DATA_TYPE_STARROCKS_INT = "INT";
    String DATA_TYPE_STARROCKS_BIGINT = "BIGINT";
    String DATA_TYPE_STARROCKS_LARGEINT = "LARGEINT";

    String DATA_TYPE_STARROCKS_FLOAT = "FLOAT";
    String DATA_TYPE_STARROCKS_DOUBLE = "DOUBLE";
    String DATA_TYPE_STARROCKS_DECIMAL = "DECIMAL";
    String DATA_TYPE_STARROCKS_DECIMALV2 = "DECIMALV2";
    String DATA_TYPE_STARROCKS_DECIMAL32 = "DECIMAL32";
    String DATA_TYPE_STARROCKS_DECIMAL64 = "DECIMAL64";
    String DATA_TYPE_STARROCKS_DECIMAL128 = "DECIMAL128";

    HashMap<LogicalTypeRoot, HashMap<String, StarRocksToJavaTrans>> DataTypeRelationMap =
            new HashMap<LogicalTypeRoot, HashMap<String, StarRocksToJavaTrans>>() {
                {
                    put(
                            LogicalTypeRoot.DATE,
                            new HashMap<String, StarRocksToJavaTrans>() {
                                {
                                    put(DATA_TYPE_STARROCKS_DATE, new VarCharVectorTrans());
                                }
                            });
                    put(
                            LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                            new HashMap<String, StarRocksToJavaTrans>() {
                                {
                                    put(DATA_TYPE_STARROCKS_DATETIME, new VarCharVectorTrans());
                                }
                            });
                    put(
                            LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                            new HashMap<String, StarRocksToJavaTrans>() {
                                {
                                    put(DATA_TYPE_STARROCKS_DATETIME, new VarCharVectorTrans());
                                }
                            });
                    put(
                            LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
                            new HashMap<String, StarRocksToJavaTrans>() {
                                {
                                    put(DATA_TYPE_STARROCKS_DATETIME, new VarCharVectorTrans());
                                }
                            });
                    put(
                            LogicalTypeRoot.VARCHAR,
                            new HashMap<String, StarRocksToJavaTrans>() {
                                {
                                    put(DATA_TYPE_STARROCKS_CHAR, new VarCharVectorTrans());
                                    put(DATA_TYPE_STARROCKS_VARCHAR, new VarCharVectorTrans());
                                    put(DATA_TYPE_STARROCKS_LARGEINT, new VarCharVectorTrans());
                                }
                            });
                    put(
                            LogicalTypeRoot.BOOLEAN,
                            new HashMap<String, StarRocksToJavaTrans>() {
                                {
                                    put(DATA_TYPE_STARROCKS_BOOLEAN, new BitVectorTrans());
                                }
                            });
                    put(
                            LogicalTypeRoot.TINYINT,
                            new HashMap<String, StarRocksToJavaTrans>() {
                                {
                                    put(DATA_TYPE_STARROCKS_TINYINT, new TinyIntVectorTrans());
                                }
                            });
                    put(
                            LogicalTypeRoot.SMALLINT,
                            new HashMap<String, StarRocksToJavaTrans>() {
                                {
                                    put(DATA_TYPE_STARROCKS_SMALLINT, new SmallIntVectorTrans());
                                }
                            });
                    put(
                            LogicalTypeRoot.INTEGER,
                            new HashMap<String, StarRocksToJavaTrans>() {
                                {
                                    put(DATA_TYPE_STARROCKS_INT, new IntVectorTrans());
                                }
                            });
                    put(
                            LogicalTypeRoot.BIGINT,
                            new HashMap<String, StarRocksToJavaTrans>() {
                                {
                                    put(DATA_TYPE_STARROCKS_BIGINT, new BigIntVectorTrans());
                                }
                            });
                    put(
                            LogicalTypeRoot.FLOAT,
                            new HashMap<String, StarRocksToJavaTrans>() {
                                {
                                    put(DATA_TYPE_STARROCKS_FLOAT, new Float4VectorTrans());
                                }
                            });
                    put(
                            LogicalTypeRoot.DOUBLE,
                            new HashMap<String, StarRocksToJavaTrans>() {
                                {
                                    put(DATA_TYPE_STARROCKS_DOUBLE, new Float8VectorTrans());
                                }
                            });
                    put(
                            LogicalTypeRoot.DECIMAL,
                            new HashMap<String, StarRocksToJavaTrans>() {
                                {
                                    put(DATA_TYPE_STARROCKS_DECIMAL, new DecimalVectorTrans());
                                    put(DATA_TYPE_STARROCKS_DECIMALV2, new DecimalVectorTrans());
                                    put(DATA_TYPE_STARROCKS_DECIMAL32, new DecimalVectorTrans());
                                    put(DATA_TYPE_STARROCKS_DECIMAL64, new DecimalVectorTrans());
                                    put(DATA_TYPE_STARROCKS_DECIMAL128, new DecimalVectorTrans());
                                }
                            });
                }
            };

    void transToJavaData(
            FieldVector curFieldVector, int rowCount, int colIndex, List<Object[]> res);

    /** StarRocks Date/Timestamp/Char/Varchar/LargeInt to java String */
    class VarCharVectorTrans implements StarRocksToJavaTrans {
        @Override
        public void transToJavaData(
                FieldVector curFieldVector, int rowCount, int colIndex, List<Object[]> res) {
            VarCharVector varCharVector = (VarCharVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                res.get(rowIndex)[colIndex] =
                        varCharVector.isNull(rowIndex)
                                ? null
                                : new String(varCharVector.get(rowIndex));
            }
        }
    }

    /** StarRocks Boolean to java boolean */
    class BitVectorTrans implements StarRocksToJavaTrans {
        @Override
        public void transToJavaData(
                FieldVector curFieldVector, int rowCount, int colIndex, List<Object[]> res) {
            BitVector bitVector = (BitVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                res.get(rowIndex)[colIndex] =
                        bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
            }
        }
    }

    /** StarRocks Tinyint to java byte */
    class TinyIntVectorTrans implements StarRocksToJavaTrans {
        @Override
        public void transToJavaData(
                FieldVector curFieldVector, int rowCount, int colIndex, List<Object[]> res) {
            TinyIntVector tinyIntVector = (TinyIntVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                res.get(rowIndex)[colIndex] =
                        tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
            }
        }
    }

    /** StarRocks Smallint to java int */
    class SmallIntVectorTrans implements StarRocksToJavaTrans {
        @Override
        public void transToJavaData(
                FieldVector curFieldVector, int rowCount, int colIndex, List<Object[]> res) {
            SmallIntVector smallIntVector = (SmallIntVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                res.get(rowIndex)[colIndex] =
                        smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
            }
        }
    }

    /** StarRocks Int to java int */
    class IntVectorTrans implements StarRocksToJavaTrans {
        @Override
        public void transToJavaData(
                FieldVector curFieldVector, int rowCount, int colIndex, List<Object[]> res) {
            IntVector intVector = (IntVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                res.get(rowIndex)[colIndex] =
                        intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
            }
        }
    }

    /** StarRocks Bigint to java long */
    class BigIntVectorTrans implements StarRocksToJavaTrans {
        @Override
        public void transToJavaData(
                FieldVector curFieldVector, int rowCount, int colIndex, List<Object[]> res) {
            BigIntVector bigIntVector = (BigIntVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                res.get(rowIndex)[colIndex] =
                        bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
            }
        }
    }

    /** StarRocks Float to java float */
    class Float4VectorTrans implements StarRocksToJavaTrans {
        @Override
        public void transToJavaData(
                FieldVector curFieldVector, int rowCount, int colIndex, List<Object[]> res) {
            Float4Vector float4Vector = (Float4Vector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                res.get(rowIndex)[colIndex] =
                        float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
            }
        }
    }

    /** StarRocks Double to java double */
    class Float8VectorTrans implements StarRocksToJavaTrans {
        @Override
        public void transToJavaData(
                FieldVector curFieldVector, int rowCount, int colIndex, List<Object[]> res) {
            Float8Vector float8Vector = (Float8Vector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                res.get(rowIndex)[colIndex] =
                        float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
            }
        }
    }

    /** StarRocks Decimal to java double */
    class DecimalVectorTrans implements StarRocksToJavaTrans {
        @Override
        public void transToJavaData(
                FieldVector curFieldVector, int rowCount, int colIndex, List<Object[]> res) {
            DecimalVector decimalVector = (DecimalVector) curFieldVector;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                res.get(rowIndex)[colIndex] =
                        decimalVector.isNull(rowIndex) ? null : decimalVector.getObject(rowIndex);
            }
        }
    }
}
