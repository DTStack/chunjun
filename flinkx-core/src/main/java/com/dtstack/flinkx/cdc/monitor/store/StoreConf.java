package com.dtstack.flinkx.cdc.monitor.store;

import java.io.Serializable;
import java.util.Map;
import java.util.StringJoiner;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/8 星期三
 */
public class StoreConf implements Serializable {
    private static final long serialVersionUID = 1L;

    private String type;

    private Map<String, Object> properties;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", StoreConf.class.getSimpleName() + "[", "]")
                .add("type='" + type + "'")
                .add("properties=" + properties)
                .toString();
    }
}
