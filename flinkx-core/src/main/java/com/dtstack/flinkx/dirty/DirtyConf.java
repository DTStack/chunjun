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

package com.dtstack.flinkx.dirty;

import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.throwable.NoRestartException;

import java.io.Serializable;
import java.util.Properties;
import java.util.StringJoiner;

/**
 * @author tiezhu@dtstack
 * @date 2021/9/22 星期三
 */
public class DirtyConf implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * This is the limit on the max consumed-data. The consumer would to be killed with throwing a
     * {@link NoRestartException} when the consumed-count exceed the limit. The default is {@link
     * Long#MAX_VALUE}, which means unlimited.
     */
    protected long maxConsumed = Long.MAX_VALUE;

    /** This is the limit on the max failed-consumed-data. Same as {@link #maxConsumed} */
    protected long maxFailedConsumed = Long.MAX_VALUE;

    /** The type of dirty-plugin. */
    private String type;

    /** Print dirty-data every ${printRate}. Defaults to no print. */
    private Long printRate = Long.MAX_VALUE;

    /** Custom parameters of different dirty-plugin. */
    private Properties pluginProperties = new Properties();

    /** Flinkx dirty-plugins local plugins path {@link Options#getFlinkLibDir()} */
    private String localPluginPath;

    public long getMaxConsumed() {
        return maxConsumed;
    }

    public void setMaxConsumed(long maxConsumed) {
        this.maxConsumed = maxConsumed;
    }

    public long getMaxFailedConsumed() {
        return maxFailedConsumed;
    }

    public void setMaxFailedConsumed(long maxFailedConsumed) {
        this.maxFailedConsumed = maxFailedConsumed;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getPrintRate() {
        return printRate;
    }

    public void setPrintRate(Long printRate) {
        this.printRate = printRate;
    }

    public Properties getPluginProperties() {
        return pluginProperties;
    }

    public void setPluginProperties(Properties pluginProperties) {
        this.pluginProperties = pluginProperties;
    }

    public String getLocalPluginPath() {
        return localPluginPath;
    }

    public void setLocalPluginPath(String localPluginPath) {
        this.localPluginPath = localPluginPath;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DirtyConf.class.getSimpleName() + "[", "]")
                .add("maxConsumed=" + maxConsumed)
                .add("maxFailedConsumed=" + maxFailedConsumed)
                .add("type='" + type + "'")
                .add("printRate=" + printRate)
                .add("pluginProperties=" + pluginProperties)
                .add("localPluginPath='" + localPluginPath + "'")
                .toString();
    }
}
