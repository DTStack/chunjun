/*
 *
 *  *
 *  *  * Licensed to the Apache Software Foundation (ASF) under one
 *  *  * or more contributor license agreements.  See the NOTICE file
 *  *  * distributed with this work for additional information
 *  *  * regarding copyright ownership.  The ASF licenses this file
 *  *  * to you under the Apache License, Version 2.0 (the
 *  *  * "License"); you may not use this file except in compliance
 *  *  * with the License.  You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 */

package com.dtstack.flinkx.connector.influxdb.converter;

import com.dtstack.flinkx.converter.AbstractRowConverter;

import com.dtstack.flinkx.converter.IDeserializationConverter;

import com.dtstack.flinkx.converter.ISerializationConverter;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2022/3/8
 */
public class InfluxdbColumnConverter extends AbstractRowConverter<RowData, RowData, Object, LogicalType> {

    public InfluxdbColumnConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public RowData toInternal(RowData input) throws Exception {
        //TODO reader
        return null;
    }

    @Override
    public Object toExternal(RowData rowData, Object output) throws Exception {
        //TODO writer
        return null;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        //TODO reader
        return super.createInternalConverter(type);
    }


    @Override
    protected ISerializationConverter createExternalConverter(LogicalType type) {
        //TODO writer
        return super.createExternalConverter(type);
    }
}
