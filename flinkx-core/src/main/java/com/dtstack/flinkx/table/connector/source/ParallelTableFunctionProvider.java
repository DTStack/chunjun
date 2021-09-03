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

package com.dtstack.flinkx.table.connector.source;

import org.apache.flink.table.connector.ParallelismProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.TableFunction;

import java.util.Optional;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/19
 */
public interface ParallelTableFunctionProvider extends TableFunctionProvider, ParallelismProvider {

    /** Helper method for creating a TableFunction provider with a provided lookup parallelism. */
    static TableFunctionProvider of(TableFunction<RowData> tableFunction, Integer parallelism) {
        return new ParallelTableFunctionProvider() {

            @Override
            public TableFunction<RowData> createTableFunction() {
                return tableFunction;
            }

            @Override
            public Optional<Integer> getParallelism() {
                return Optional.ofNullable(parallelism);
            }
        };
    }
}
