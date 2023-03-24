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

package com.dtstack.chunjun.connector.starrocks.source;

import com.dtstack.chunjun.connector.starrocks.config.StarRocksConfig;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;

import com.google.common.base.Preconditions;
import org.apache.commons.collections.CollectionUtils;

public class StarRocksInputFormatBuilder extends BaseRichInputFormatBuilder<StarRocksInputFormat> {
    public StarRocksInputFormatBuilder(StarRocksInputFormat format) {
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
        Preconditions.checkArgument(
                CollectionUtils.isNotEmpty(conf.getFeNodes()), "starRocks feNodes is required");
        Preconditions.checkNotNull(
                conf.getDatabase(), "starRocks database is required when nameMapped is not enable");
        Preconditions.checkNotNull(
                conf.getTable(), "starRocks table is required when nameMapped is not enable");
        Preconditions.checkNotNull(conf.getUsername(), "starRocks username is required");
        Preconditions.checkNotNull(conf.getPassword(), "starRocks password is required");
    }
}
