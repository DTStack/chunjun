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

package com.dtstack.chunjun.ddl.convent.oracle;

import com.dtstack.chunjun.cdc.DdlRowData;
import com.dtstack.chunjun.cdc.ddl.DdlConvent;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnOperator;
import com.dtstack.chunjun.cdc.ddl.definition.ConstraintOperator;
import com.dtstack.chunjun.cdc.ddl.definition.DdlOperator;
import com.dtstack.chunjun.cdc.ddl.definition.IndexOperator;
import com.dtstack.chunjun.cdc.ddl.definition.TableOperator;
import com.dtstack.chunjun.mapping.MappingConfig;
import com.dtstack.chunjun.throwable.ConventException;

import java.util.Collections;
import java.util.List;

public class OracleDdlConventImpl implements DdlConvent {
    private final OperatorConventImpl operatorConvent = new OperatorConventImpl();

    public OracleDdlConventImpl() {
        this(null);
    }

    public OracleDdlConventImpl(MappingConfig mappingConfig) {}

    @Override
    public List<DdlOperator> rowConventToDdlData(DdlRowData row) throws ConventException {
        throw new ConventException(
                row.getSql(),
                new UnsupportedOperationException("oracle plugin not support parse ddl sql"));
    }

    @Override
    public List<String> ddlDataConventToSql(DdlOperator ddlOperator) throws ConventException {
        try {
            if (ddlOperator instanceof TableOperator) {
                return operatorConvent.conventOperateTable((TableOperator) ddlOperator);
            } else if (ddlOperator instanceof ColumnOperator) {
                return operatorConvent.conventOperateColumn((ColumnOperator) ddlOperator);
            } else if (ddlOperator instanceof IndexOperator) {
                return Collections.singletonList(
                        operatorConvent.conventOperateIndex((IndexOperator) ddlOperator));
            } else if (ddlOperator instanceof ConstraintOperator) {
                return Collections.singletonList(
                        operatorConvent.conventOperatorConstraint(
                                (ConstraintOperator) ddlOperator));
            }
        } catch (Exception t) {
            throw new ConventException(ddlOperator.getSql(), t);
        }
        throw new ConventException(
                ddlOperator.getSql(),
                new RuntimeException("not support convent" + ddlOperator.sql));
    }

    @Override
    public String getDataSourceType() {
        return "oracle";
    }

    @Override
    public List<String> map(DdlRowData row) {
        throw new UnsupportedOperationException("oracle plugin not support parse ddl sql");
    }
}
