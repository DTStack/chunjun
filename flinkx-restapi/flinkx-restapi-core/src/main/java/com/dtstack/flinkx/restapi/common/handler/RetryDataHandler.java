package com.dtstack.flinkx.restapi.common.handler;

import com.dtstack.flinkx.restapi.common.MapUtils;
import com.dtstack.flinkx.restapi.common.exception.ResponseRetryException;

import java.util.Map;
import java.util.Set;

public class RetryDataHandler extends DataHandler {

    public RetryDataHandler(String key, Set<String> value) {
        super(key, value);
    }

    @Override
    public void execute(Map<String, Object> responseData) {
        String[] strings = new String[0];
        strings[0] = key;
        Object data = MapUtils.getData(responseData, strings);
        if (value.contains(data.toString())) {
            throw new ResponseRetryException("key:"+key+" contains"+data.toString()+" ,need retry");
        }
    }
}
