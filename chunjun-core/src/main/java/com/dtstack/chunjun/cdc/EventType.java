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

package com.dtstack.chunjun.cdc;

public enum EventType {
    UNKNOWN,

    CREATE_DATABASE,
    DROP_DATABASE,

    CREATE_SCHEMA,

    DROP_SCHEMA,

    CREATE_TABLE,

    DROP_TABLE,

    ADD_COLUMN,

    DROP_COLUMN,

    RENAME_COLUMN,

    ADD_INDEX,

    DROP_INDEX,

    ADD_FOREIGN_KEY,

    DROP_FOREIGN_KEY,

    TRUNCATE_TABLE,

    MODIFY_COLUMN,

    REBASE_AUTO_ID,

    RENAME_TABLE,

    SET_DEFAULT_VALUE,

    SHARD_ROW_ID,

    ALTER_TABLE_COMMENT,

    RENAME_INDEX,

    RENAME_CONSTRAINT,

    ADD_TABLE_PARTITION,

    DROP_TABLE_PARTITION,

    CREATE_VIEW,

    MODIFY_TABLE_CHARSET_AND_COLLATE,

    TRUNCATE_TABLE_PARTITION,

    DROP_VIEW,

    RECOVER_TABLE,

    MODIFY_SCHEMA_CHARSET_AND_COLLATE,

    LOCK_TABLE,

    UNLOCK_TABLE,

    REPAIR_TABLE,

    SET_TIFLASH_REPLICA,

    UPDATE_TIFLASH_REPLICA_STATUS,

    ADD_PRIMARY_KEY,

    DROP_PRIMARY_KEY,

    CREATE_SEQUENCE,

    ALTER_SEQUENCE,

    DROP_SEQUENCE,

    ALTER_TABLE,

    ALTER_COLUMN,

    CREATE_INDEX,

    CREATE_USER,

    DROP_USER,

    DROP_CONSTRAINT,

    ADD_CONSTRAINT,

    ALTER_CONSTRAINT_ENFORCED,

    // DML
    INSERT,

    DELETE,

    UPDATE,

    ALTER,

    RENAME,

    QUERY,

    TRUNCATE,

    DROP,

    CREATE,

    ERASE,

    CINDEX,

    DINDEX,

    GTID,

    XACOMMIT,

    XAROLLBACK,

    MHEARTBEAT,
    ;

    public static boolean contains(String type) {
        EventType[] eventTypes = values();
        for (EventType eventType : eventTypes) {
            if (eventType.name().equalsIgnoreCase(type)) {
                return true;
            }
        }
        return false;
    }

    public boolean isDelete() {
        return this.equals(DELETE);
    }

    public boolean isUpdate() {
        return this.equals(UPDATE);
    }

    public boolean isInsert() {
        return this.equals(INSERT);
    }

    public boolean isQuery() {
        return this.equals(QUERY);
    }

    public boolean isDML() {
        return isDelete() || isInsert() || isUpdate() || isQuery();
    }

    public boolean isDDL() {
        return !isDML();
    }
}
