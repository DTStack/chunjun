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

package com.dtstack.flinkx.sink.format;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.converter.AbstractRowConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The builder of RichOutputFormat
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public abstract class BaseRichOutputFormatBuilder {
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected BaseRichOutputFormat format;

    public void setConfig(FlinkxCommonConf config) {
        format.setConfig(config);
    }

    public void setInitAccumulatorAndDirty(boolean initAccumulatorAndDirty) {
        this.format.initAccumulatorAndDirty = initAccumulatorAndDirty;
    }

    public void setRowConverter(AbstractRowConverter rowConverter) {
        format.setRowConverter(rowConverter);
    }

    /** Check the value of parameters */
    protected abstract void checkFormat();

    public BaseRichOutputFormat finish() {
        checkFormat();

        /**
         * 200000条限制的原因： 按照目前的使用情况以及部署配置，假设写入字段数量平均为50个，一个单slot的TaskManager内存为1G，
         * 在不考虑各插件批量写入对内存特殊要求并且只考虑插件缓存这么多条数据的情况下，batchInterval为400000条时出现fullGC，
         * 为了避免fullGC以及OOM，并且保证batchInterval有足够的配置空间，取最大值的一半200000。
         */
        if (this.format.getConfig().getBatchSize() > ConstantValue.MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("批量写入条数必须小于[200000]条");
        }

        return format;
    }
}
