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

package com.dtstack.flinkx.outputformat;

import com.dtstack.flinkx.config.RestoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;

/**
 * The builder of RichOutputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class BaseRichOutputFormatBuilder {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());
    protected BaseRichOutputFormat format;

    public void setDirtyPath(String dirtyPath) {
        format.setDirtyPath(dirtyPath);
    }

    public void setDirtyHadoopConfig(Map<String,Object> dirtyHadoopConfig) {
        format.setDirtyHadoopConfig(dirtyHadoopConfig);
    }

    public void setSrcCols(List<String> srcCols) {
        format.setSrcFieldNames(srcCols);
    }

    public void setErrors(Integer errors) {
        format.errors = errors;
    }

    public void setErrorRatio(Double errorRatio) {
        format.errorRatio = errorRatio;
    }

    public void setMonitorUrls(String monitorUrl) {
        format.monitorUrl = monitorUrl;
    }

    public void setBatchInterval(int batchInterval) {
        format.batchInterval = batchInterval;
    }

    public void setRestoreConfig(RestoreConfig restoreConfig){
        format.restoreConfig = restoreConfig;
    }

    public void setInitAccumulatorAndDirty(boolean initAccumulatorAndDirty) {
        this.format.initAccumulatorAndDirty = initAccumulatorAndDirty;
    }

    /**
     * Check the value of parameters
     */
    protected abstract void checkFormat();

    public BaseRichOutputFormat finish() {
        checkFormat();
        return format;
    }

}
