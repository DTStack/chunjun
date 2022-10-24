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

package com.dtstack.chunjun.ddl.convent.mysql.parse.enums;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;

public enum AlterInstanceActionEnum {
    ENABLE_INNODB_REDO_LOG("ENABLE INNODB REDO_LOG"),
    DISABLE_INNODB_REDO_LOG("DISABLE INNODB REDO_LOG"),
    ROTATE_INNODB_MASTER_KEY("ROTATE INNODB MASTER KEY"),
    ROTATE_BINLOG_MASTER_KEY("ROTATE BINLOG MASTER KEY"),
    RELOAD_TLS("RELOAD TLS"),
    RELOAD_TLS_FOR_CHANNEL("RELOAD TLS FOR CHANNEL"),
    RELOAD_TLS_NO_ROLLBACK_ON_ERROR("RELOAD TLS NO ROLLBACK ON ERROR"),
    RELOAD_KEYRING("RELOAD KEYRING"),
    ;

    private String digest;

    AlterInstanceActionEnum(String digest) {
        this.digest = digest;
    }

    public String getDigest() {
        return digest;
    }

    @Override
    public String toString() {
        return digest;
    }

    public SqlLiteral symbol(SqlParserPos pos) {
        return SqlLiteral.createSymbol(this, pos);
    }
}
