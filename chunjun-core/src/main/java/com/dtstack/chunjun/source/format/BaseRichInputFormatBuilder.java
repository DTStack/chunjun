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

package com.dtstack.chunjun.source.format;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;

import com.google.common.base.Preconditions;

/** Abstract specification for all the InputFormatBuilder implementation */
public abstract class BaseRichInputFormatBuilder<T extends BaseRichInputFormat> {

    protected T format;

    public BaseRichInputFormatBuilder(T format) {
        this.format = format;
    }

    public void setConfig(CommonConfig config) {
        format.setConfig(config);
    }

    public void setRowConverter(AbstractRowConverter rowConverter) {
        setRowConverter(rowConverter, false);
    }

    public void setRowConverter(AbstractRowConverter rowConverter, boolean useAbstractColumn) {
        format.setRowConverter(rowConverter);
        format.setUseAbstractColumn(useAbstractColumn);
    }

    /** 检查format配置 */
    protected abstract void checkFormat();

    /**
     * 结束builder构建
     *
     * @return 返回format对象
     */
    public BaseRichInputFormat finish() {
        Preconditions.checkNotNull(format);
        if (format.getConfig().isCheckFormat()) {
            checkFormat();
        }
        return format;
    }
}
