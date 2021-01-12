package com.dtstack.flinkx.restapi.common.handler;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class DataHandlerFactory {


    private static HashMap<String, DataHandler> handlerHashMap = new HashMap<>(16);

    static {

    }

    public static DataHandler getDataHandler(Map dataHandlerParam) {
        if (StringUtils.isNotBlank(dataHandlerParam.get("key").toString())) {
            return handlerHashMap.get(dataHandlerParam.get("key").toString());
        }else{
            throw new IllegalArgumentException("dataHandler key not allow blank");
        }
    }

    public static void destroy() {
        //gc
        handlerHashMap = null;
    }
}
