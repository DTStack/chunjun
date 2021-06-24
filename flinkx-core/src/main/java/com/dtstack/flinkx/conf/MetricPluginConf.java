package com.dtstack.flinkx.conf;

import java.util.Map;

public class MetricPluginConf {

    private String pluginName;

    private Map<String,Object> pluginProp;

    public String getPluginName() {
        return pluginName;
    }

    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }

    public Map<String, Object> getPluginProp() {
        return pluginProp;
    }

    public void setPluginProp(Map<String, Object> pluginProp) {
        this.pluginProp = pluginProp;
    }
}
