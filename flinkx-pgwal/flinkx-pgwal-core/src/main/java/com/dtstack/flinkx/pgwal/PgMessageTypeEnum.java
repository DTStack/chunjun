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
package com.dtstack.flinkx.pgwal;

/**
 * Date: 2019/12/14
 * Company: www.dtstack.com
 *
 * reference to https://github.com/debezium/debezium & http://www.postgres.cn/docs/10/protocol-logicalrep-message-formats.html
 *
 * @author tudou
 */
public enum PgMessageTypeEnum {
    RELATION,
    BEGIN,
    COMMIT,
    INSERT,
    UPDATE,
    DELETE,
    TYPE,
    ORIGIN;

    public static PgMessageTypeEnum forType(char type) {
        switch (type) {
            case 'R': return RELATION;
            case 'B': return BEGIN;
            case 'C': return COMMIT;
            case 'I': return INSERT;
            case 'U': return UPDATE;
            case 'D': return DELETE;
            case 'Y': return TYPE;
            case 'O': return ORIGIN;
            default: throw new IllegalArgumentException("Unsupported message type: " + type);
        }
    }
}
