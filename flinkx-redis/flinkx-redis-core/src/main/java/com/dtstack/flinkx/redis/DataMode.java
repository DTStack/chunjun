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

package com.dtstack.flinkx.redis;

/**
 * Operation type of redis database
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public enum  DataMode {

    /** reader mode */

    /** write mode */
    SET("set"),L_PUSH("lpush"),R_PUSH("rpush"),S_ADD("sadd"),Z_ADD("zadd"),H_SET("hset");

    private String mode;

    DataMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

    public static DataMode getDataMode(String mode){
        for (DataMode dataMode : DataMode.values()) {
            if(dataMode.getMode().equals(mode)){
                return dataMode;
            }
        }

        throw new RuntimeException("Unsupported redis data mode:" + mode);
    }
}
