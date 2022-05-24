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
package com.dtstack.chunjun.connector.hbase14.source;

import com.dtstack.chunjun.connector.hbase.conf.HBaseConf;
import com.dtstack.chunjun.connector.hbase.conf.HBaseConfigConstants;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;

import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * The builder of HbaseInputFormat
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class HBaseInputFormatBuilder extends BaseRichInputFormatBuilder {

    public HBaseInputFormatBuilder() {
        super(new HBaseInputFormat());
    }

    public void setHbaseConfig(Map<String, Object> hbaseConfig) {
        ((HBaseInputFormat) format).hbaseConfig = hbaseConfig;
    }

    public void sethHBaseConf(HBaseConf hBaseConf) {
        ((HBaseInputFormat) format).hBaseConf = hBaseConf;
    }

    @Override
    protected void checkFormat() {
        Preconditions.checkArgument(
                ((HBaseInputFormat) format).hBaseConf.getScanCacheSize()
                                <= HBaseConfigConstants.MAX_SCAN_CACHE_SIZE
                        && ((HBaseInputFormat) format).hBaseConf.getScanCacheSize()
                                >= HBaseConfigConstants.MIN_SCAN_CACHE_SIZE,
                "scanCacheSize should be between "
                        + HBaseConfigConstants.MIN_SCAN_CACHE_SIZE
                        + " and "
                        + HBaseConfigConstants.MAX_SCAN_CACHE_SIZE);
    }
}
