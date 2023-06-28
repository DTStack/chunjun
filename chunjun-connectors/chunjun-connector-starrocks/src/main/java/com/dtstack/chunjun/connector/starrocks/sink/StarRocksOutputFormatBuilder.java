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

package com.dtstack.chunjun.connector.starrocks.sink;

import com.dtstack.chunjun.connector.starrocks.config.StarRocksConfig;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import com.google.common.base.Preconditions;

public class StarRocksOutputFormatBuilder
        extends BaseRichOutputFormatBuilder<StarRocksOutputFormat> {

    public StarRocksOutputFormatBuilder(StarRocksOutputFormat format) {
        super(format);
    }

    public void setStarRocksConf(StarRocksConfig starRocksConfig) {
        super.setConfig(starRocksConfig);
        format.setStarRocksConf(starRocksConfig);
    }

    @Override
    protected void checkFormat() {
        StarRocksConfig conf = format.getStarRocksConf();
        Preconditions.checkNotNull(conf.getUrl(), "starRocks url is required");
        Preconditions.checkNotNull(conf.getFeNodes(), "starRocks feNodes is required");
        if (!conf.isNameMapped()) {
            Preconditions.checkNotNull(
                    conf.getDatabase(),
                    "starRocks database is required when nameMapped is not enable");
            Preconditions.checkNotNull(
                    conf.getTable(), "starRocks table is required when nameMapped is not enable");
            // cache is not necessary because we only have one table
            conf.setCacheTableStruct(false);
            conf.setCheckStructFirstTime(false);
        }
        Preconditions.checkNotNull(conf.getUsername(), "starRocks username is required");
        Preconditions.checkNotNull(conf.getPassword(), "starRocks password is required");
    }
}
