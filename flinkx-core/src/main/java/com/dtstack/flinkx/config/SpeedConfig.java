/**
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

package com.dtstack.flinkx.config;

import java.util.HashMap;
import java.util.Map;

/**
 * The configuration of speed control
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class SpeedConfig extends AbstractConfig {

    public static final String KEY_BYTES = "bytes";
    public static final String KEY_NUM_CHANNELS = "channel";

    public static final long DEFAULT_SPEED_BYTES = Long.MAX_VALUE;
    public static final int DEFAULT_NUM_CHANNALS = 1;

    public SpeedConfig(Map<String, Object> map) {
        super(map);
    }

    public static SpeedConfig defaultConfig(){
        Map<String, Object> map = new HashMap<>(2);
        map.put("bytes",DEFAULT_SPEED_BYTES);
        map.put("channel",DEFAULT_NUM_CHANNALS);
        return new SpeedConfig(map);
    }

    public long getBytes() {
        return getLongVal(KEY_BYTES, DEFAULT_SPEED_BYTES);
    }

    public void setBytes(long bytes) {
        setLongVal(KEY_BYTES, bytes);
    }

    public int getChannel() {
        return getIntVal(KEY_NUM_CHANNELS, DEFAULT_NUM_CHANNALS);
    }

    public void setChannel(int channel) {
        setIntVal(KEY_NUM_CHANNELS, channel);
    }

}
