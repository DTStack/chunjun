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

package com.dtstack.flinkx.conf;

import java.io.Serializable;
import java.util.Map;

/**
 * @author: shifang
 * @description metric pluginName &properties
 * @date: 2021/6/28 下午5:09
 */
public class MetricPluginConf implements Serializable {

    private static final long serialVersionUID = 1L;

    private String pluginName = "prometheus";

    private String rowSizeCalculatorType = "objectSizeCalculator";

    private Map<String, Object> pluginProp;

    public String getPluginName() {
        return pluginName;
    }

    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }

    public String getRowSizeCalculatorType() {
        return rowSizeCalculatorType;
    }

    public void setRowSizeCalculatorType(String rowSizeCalculatorType) {
        this.rowSizeCalculatorType = rowSizeCalculatorType;
    }

    public Map<String, Object> getPluginProp() {
        return pluginProp;
    }

    public void setPluginProp(Map<String, Object> pluginProp) {
        this.pluginProp = pluginProp;
    }

    @Override
    public String toString() {
        return "MetricPluginConf{"
                + "pluginName='"
                + pluginName
                + '\''
                + ", pluginProp="
                + pluginProp
                + '}';
    }
}
