package com.dtstack.flinkx.restapi.common.handler;

import com.dtstack.flinkx.restapi.common.MapUtils;
import com.dtstack.flinkx.restapi.common.exception.ResponseBreakException;
import com.dtstack.flinkx.restapi.common.exception.ResponseRetryException;

import java.util.Map;
import java.util.Set;

public class BreakDataHandler extends DataHandler {

    public BreakDataHandler(String key, Set<String> value) {
        super(key, value);
    }

    @Override
    public void execute(Map<String, Object> responseData) {
        String[] strings = new String[0];
        strings[0] = key;
        Object data = MapUtils.getData(responseData, strings);
        if (value.contains(data.toString())) {
            throw new ResponseBreakException("key:"+key+" contains"+data.toString()+" ,need break");
        }

    }
}
