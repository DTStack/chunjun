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
package com.dtstack.chunjun.connector.pgwal.inputformat;

import com.dtstack.chunjun.connector.pgwal.conf.PGWalConf;
import com.dtstack.chunjun.connector.pgwal.util.PGUtil;
import com.dtstack.chunjun.converter.AbstractCDCRowConverter;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;
import com.dtstack.chunjun.util.ClassUtil;
import com.dtstack.chunjun.util.GsonUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;

/** */
public class PGWalInputFormatBuilder extends BaseRichInputFormatBuilder<PGWalInputFormat> {

    public PGWalInputFormatBuilder() {
        super(new PGWalInputFormat());
    }

    public void setConf(PGWalConf conf) {
        setConfig(conf);
        format.setConf(conf);
    }

    public void setRowConverter(AbstractCDCRowConverter rowConverter) {
        this.format.setRowConverter(rowConverter);
    }

    @Override
    protected void checkFormat() {
        StringBuilder sb = new StringBuilder(256);
        final PGWalConf conf = format.getConf();

        if (conf.getParallelism() > 1) {
            sb.append("binLog can not support channel bigger than 1, current channel is [")
                    .append(conf.getParallelism())
                    .append("];\n");
        }

        if (StringUtils.isNotEmpty(conf.getCat())) {
            HashSet<String> set = Sets.newHashSet("INSERT", "UPDATE", "DELETE");
            List<String> cats = Lists.newArrayList(conf.getCat().toUpperCase().split(","));
            cats.removeIf(s -> set.contains(s.toUpperCase(Locale.ENGLISH)));
            if (CollectionUtils.isNotEmpty(cats)) {
                sb.append("binlog cat not support-> ")
                        .append(GsonUtil.GSON.toJson(cats))
                        .append(",just support->")
                        .append(GsonUtil.GSON.toJson(set))
                        .append(";\n");
            }
        }

        ClassUtil.forName(PGUtil.DRIVER_NAME, getClass().getClassLoader());
    }
}
