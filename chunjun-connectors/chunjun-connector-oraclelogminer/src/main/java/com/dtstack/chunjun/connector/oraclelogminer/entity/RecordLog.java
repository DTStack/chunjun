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

package com.dtstack.chunjun.connector.oraclelogminer.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.math.BigInteger;

@Getter
@Setter
@AllArgsConstructor
public class RecordLog {

    private BigInteger scn;
    /** undo语句* */
    private String sqlUndo;
    /** redo语句* */
    private String sqlRedo;
    /** 事务id撤销段号* */
    private String xidUsn;
    /** 事务id槽号* */
    private String xidSlt;
    /** 事务id序列号* */
    private String xidSqn;
    /** rowId* */
    private String rowId;

    private String tableName;
    /** 是否发生了日志切割* */
    private boolean hasMultiSql;
    /** DML操作类型 1插入 2删除 3 更新* */
    private int operationCode;
}
