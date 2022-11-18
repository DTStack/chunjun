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

import lombok.Data;

@Data
public class OracleInfo {

    /** 数据库版本 * */
    private int version;
    /** 是否是rac模式 * */
    private boolean isRacMode;
    /** 是否是cdb模式 * */
    private boolean isCdbMode;
    /** 数据库字符编码 * */
    private String encoding;

    public boolean isGbk() {
        return encoding.contains("GBK");
    }

    public boolean isOracle10() {
        return version == 10;
    }
}
