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

package com.dtstack.chunjun.connector.oraclelogminer.entity;

import com.dtstack.chunjun.connector.oraclelogminer.util.SqlUtil;

import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;

@Getter
@AllArgsConstructor
public class ColumnInfo {
    private final Set<String> charType = Sets.newHashSet("CHAR", "NVARCHAR2", "VARCHAR2", "NCHAR");

    private final String name;
    private final String type;
    private final Integer precision;
    private final Integer charLength;
    private final Integer dataLength;
    private final Integer scale;
    private final String defaultValue;
    private final boolean nullAble;
    private final String comment;
    private final boolean pk;

    public String conventToSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(SqlUtil.quote(name, "\"")).append(" ").append(type);
        if (Objects.nonNull(charLength) && isCharFamily()) {
            sb.append("(").append(charLength).append(")");
        } else if (type.equals("NUMBER")) {
            sb.append("(").append(precision);
            if (scale != null) {
                sb.append(",").append(scale);
            }
            sb.append(")");
        } else if (type.equals("FLOAT")) {
            sb.append("(").append(precision).append(")");
        } else if (type.equals("RAW")) {
            if (dataLength != null) {
                sb.append("(").append(dataLength).append(")");
            }
        }
        sb.append(" ");
        if (StringUtils.isNotBlank(defaultValue)) {
            sb.append("default ").append(defaultValue).append(" ");
        }

        if (!nullAble) {
            sb.append("NOT").append(" ").append("NULL").append(" ");
        }

        if (pk) {
            sb.append("constraint")
                    .append(" ")
                    .append("chunjun_pk")
                    .append(UUID.randomUUID().toString().trim().replace("-", ""), 0, 12)
                    .append(" primary key");
        }

        return sb.toString();
    }

    public String getCommentSql() {
        if (comment != null) {
            return "COMMENT ON COLUMN "
                    + SqlUtil.quote(getName(), "\"")
                    + " IS "
                    + SqlUtil.quote(comment, "'");
        }
        return null;
    }

    private boolean isCharFamily() {
        return charType.contains(type);
    }
}
