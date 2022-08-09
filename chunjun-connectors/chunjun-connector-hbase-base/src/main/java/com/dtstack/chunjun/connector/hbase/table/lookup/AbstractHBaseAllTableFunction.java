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

package com.dtstack.chunjun.connector.hbase.table.lookup;

import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.conf.HBaseConf;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.lookup.AbstractAllTableFunction;
import com.dtstack.chunjun.lookup.conf.LookupConf;

public abstract class AbstractHBaseAllTableFunction extends AbstractAllTableFunction {

    protected final HBaseTableSchema hbaseTableSchema;
    protected final HBaseConf hBaseConf;

    protected String nullStringLiteral;

    public AbstractHBaseAllTableFunction(
            String[] fieldNames,
            String[] keyNames,
            LookupConf lookupConf,
            AbstractRowConverter rowConverter,
            HBaseTableSchema hbaseTableSchema,
            HBaseConf hBaseConf) {
        super(fieldNames, keyNames, lookupConf, rowConverter);
        this.hbaseTableSchema = hbaseTableSchema;
        this.hBaseConf = hBaseConf;
        this.nullStringLiteral = hBaseConf.getNullStringLiteral();
    }
}
