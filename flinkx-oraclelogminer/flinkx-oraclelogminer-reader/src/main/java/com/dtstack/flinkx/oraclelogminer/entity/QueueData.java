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
package com.dtstack.flinkx.oraclelogminer.entity;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Date: 2020/06/01
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class QueueData {
    private BigDecimal scn;
    private Map<String, Object> data;

    public QueueData(BigDecimal lsn, Map<String, Object> data) {
        this.scn = lsn;
        this.data = data;
    }

    public BigDecimal getScn() {
        return scn;
    }

    public Map<String, Object> getData() {
        return data;
    }

    @Override
    public String toString() {
        return "QueueData{" +
                "scn=" + scn +
                ", data=" + data +
                '}';
    }
}
