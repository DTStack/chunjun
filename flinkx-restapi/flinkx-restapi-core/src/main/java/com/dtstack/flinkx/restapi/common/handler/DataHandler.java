package com.dtstack.flinkx.restapi.common.handler;

import java.util.Map;
import java.util.Set;

public abstract class DataHandler {

    protected String key;

    protected Set<String> value;

    public DataHandler(String key, Set<String> value) {
        this.key = key;
        this.value = value;
    }

    public boolean isPipei(Map<String, Object> responseData) {
        return responseData.containsKey(key) && value.contains(responseData.get(key).toString());
    }

    public abstract void execute(Map<String, Object> responseData);

}
