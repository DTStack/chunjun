package com.dtstack.flinkx.conf;

import java.io.Serializable;
import java.util.Map;

public class MetricPluginConf implements Serializable {

    private static final long serialVersionUID = 1L;

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
