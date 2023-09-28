/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.converter;

import com.dtstack.chunjun.config.TypeConfig;

import org.apache.flink.table.types.DataType;

/**
 * Each connector implements. It is used to convert raw type to flink type.
 *
 * <p>e.g.: convert string "SHORT" to {@link DataType}.
 */
@FunctionalInterface
public interface RawTypeMapper {

    /**
     * @param type raw type string. e.g.: "SHORT", "INT", "TIMESTAMP"
     * @return e.g.: DataTypes.INT(), DataTypes.TIMESTAMP().
     */
    DataType apply(TypeConfig type);
}
