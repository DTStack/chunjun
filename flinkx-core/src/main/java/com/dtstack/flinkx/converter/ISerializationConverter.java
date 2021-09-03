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
package com.dtstack.flinkx.converter;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;

/**
 * Date: 2021/04/30 Company: www.dtstack.com
 *
 * @author tudou
 */
public interface ISerializationConverter<T> extends Serializable {

    /**
     * 类型T一般是 Object，HBase这种特殊的就是byte[]
     *
     * @param rowData
     * @param pos
     * @param output
     * @throws Exception
     */
    void serialize(RowData rowData, int pos, T output) throws Exception;
}
