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
    public static final String KEY_NUM_READER_CHANNELS = "readerChannel";
    public static final String KEY_NUM_WRITER_CHANNELS = "writerChannel";
    public static final String KEY_REBALANCE = "rebalance";

    public static final long DEFAULT_SPEED_BYTES = Long.MAX_VALUE;
    public static final int DEFAULT_NUM_CHANNELS = 1;
    public static final int DEFAULT_NUM_READER_WRITER_CHANNEL = -1;

    public SpeedConfig(Map<String, Object> map) {
        super(map);
    }

    public static SpeedConfig defaultConfig(){
        Map<String, Object> map = new HashMap<>(2);
        map.put(KEY_BYTES, DEFAULT_SPEED_BYTES);
        map.put(KEY_NUM_CHANNELS, DEFAULT_NUM_CHANNELS);
        map.put(KEY_NUM_READER_CHANNELS, DEFAULT_NUM_READER_WRITER_CHANNEL);
        map.put(KEY_NUM_WRITER_CHANNELS, DEFAULT_NUM_READER_WRITER_CHANNEL);
        map.put(KEY_REBALANCE, false);
        return new SpeedConfig(map);
    }

    public long getBytes() {
        return getLongVal(KEY_BYTES, DEFAULT_SPEED_BYTES);
    }

    public void setBytes(long bytes) {
        setLongVal(KEY_BYTES, bytes);
    }

    public int getChannel() {
        return getIntVal(KEY_NUM_CHANNELS, DEFAULT_NUM_CHANNELS);
    }

    public int getReaderChannel(){
        return getIntVal(KEY_NUM_READER_CHANNELS, DEFAULT_NUM_READER_WRITER_CHANNEL);
    }

    public int getWriterChannel(){
        return getIntVal(KEY_NUM_WRITER_CHANNELS, DEFAULT_NUM_READER_WRITER_CHANNEL);
    }

    public void setChannel(int channel) {
        setIntVal(KEY_NUM_CHANNELS, channel);
    }

    public boolean isRebalance() {
        return getBooleanVal(KEY_REBALANCE, false);
    }
}
