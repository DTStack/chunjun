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

package com.dtstack.flinkx.config;

import com.dtstack.flinkx.util.GsonUtil;
import com.google.gson.internal.LinkedTreeMap;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Abstract Config
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public abstract class AbstractConfig implements Serializable {

    protected Map<String,Object> internalMap;

    public AbstractConfig(Map<String,Object> map) {
        if(map != null) {
            internalMap = map;
        } else {
            internalMap = new HashMap<>();
        }
    }

    public Map<String,Object> getAll(){
        return internalMap;
    }

    public void setVal(String key, Object value) {
        internalMap.put(key, value);
    }

    public void setStringVal(String key, String value) {
        setVal(key, value);
    }

    public void setBooleanVal(String key, boolean value) {
        setVal(key, value);
    }

    public void setIntVal(String key, int value) {
        setVal(key, value);
    }

    public void setLongVal(String key, long value) {
        setVal(key, value);
    }

    public void setDoubleVal(String key, double value) {
        setVal(key, value);
    }

    public Object getVal(String key) {
        Object obj = internalMap.get(key);
        if (obj instanceof LinkedTreeMap) {
            LinkedTreeMap treeMap = (LinkedTreeMap) obj;
            Map<String, Object> map = new HashMap<>(Math.max((int) (treeMap.size()/.75f) + 1, 16));
            map.putAll(treeMap);
            return map;
        }
        return obj;
    }

    public Object getVal(String key, Object defaultValue) {
        Object ret = getVal(key);
        if(ret == null) {
            return defaultValue;
        }
        return ret;
    }

    public String getStringVal(String key) {
        return (String) internalMap.get(key);
    }

    public String getStringVal(String key, String defaultValue) {
        String ret = getStringVal(key);
        if(ret == null || ret.trim().length() == 0) {
            return defaultValue;
        }
        return ret;
    }

    public int getIntVal(String key, int defaultValue) {
        Object ret = internalMap.get(key);
        if(ret == null) {
            return defaultValue;
        }
        if(ret instanceof Integer) {
            return (Integer) ret;
        }
        if(ret instanceof String) {
            return Integer.parseInt((String) ret);
        }
        if(ret instanceof Long) {
            return ((Long)ret).intValue();
        }
        if(ret instanceof Float) {
            return ((Float)ret).intValue();
        }
        if(ret instanceof Double) {
            return ((Double)ret).intValue();
        }
        if(ret instanceof BigInteger) {
            return ((BigInteger)ret).intValue();
        }
        if(ret instanceof BigDecimal) {
            return ((BigDecimal)ret).intValue();
        }
        throw new RuntimeException(String.format("cant't %s from %s to int, internalMap = %s", key, ret.getClass().getName(), GsonUtil.GSON.toJson(internalMap)));
    }

    public long getLongVal(String key, long defaultValue) {
        Object ret = internalMap.get(key);
        if(ret == null) {
            return defaultValue;
        }
        if(ret instanceof Long) {
            return ((Long)ret);
        }
        if(ret instanceof Integer) {
            return ((Integer)ret).longValue();
        }
        if(ret instanceof String) {
            return Long.parseLong((String)ret);
        }
        if(ret instanceof Float) {
            return ((Float)ret).longValue();
        }
        if(ret instanceof Double) {
            return ((Double)ret).longValue();
        }
        if(ret instanceof BigInteger) {
            return ((BigInteger)ret).longValue();
        }
        if(ret instanceof BigDecimal) {
            return ((BigDecimal)ret).longValue();
        }
        throw new RuntimeException(String.format("cant't %s from %s to long, internalMap = %s", key, ret.getClass().getName(), GsonUtil.GSON.toJson(internalMap)));
    }

    public double getDoubleVal(String key, double defaultValue) {
        Object ret = internalMap.get(key);
        if (ret == null) {
            return defaultValue;
        }
        if (ret instanceof Double) {
            return ((Double) ret);
        }
        if (ret instanceof Long) {
            return ((Long) ret).doubleValue();
        }
        if (ret instanceof Integer) {
            return ((Integer) ret).doubleValue();
        }
        if (ret instanceof String) {
            return Double.parseDouble((String) ret);
        }
        if (ret instanceof Float) {
            return ((Float) ret).doubleValue();
        }
        if (ret instanceof BigInteger) {
            return ((BigInteger) ret).doubleValue();
        }
        if (ret instanceof BigDecimal) {
            return ((BigDecimal) ret).doubleValue();
        }
        throw new RuntimeException(String.format("cant't %s from %s to double, internalMap = %s", key, ret.getClass().getName(), GsonUtil.GSON.toJson(internalMap)));
    }


    public boolean getBooleanVal(String key, boolean defaultValue) {
        Object ret = internalMap.get(key);
        if (ret == null) {
            return defaultValue;
        }
        if (ret instanceof Boolean) {
            return (Boolean) ret;
        }
        throw new RuntimeException(String.format("cant't %s from %s to boolean, internalMap = %s", key, ret.getClass().getName(), GsonUtil.GSON.toJson(internalMap)));
    }

    /**
     * 从指定key中获取Properties配置信息
     * @param key
     * @param p
     * @return
     */
    @SuppressWarnings("unchecked")
    public Properties getProperties(String key, Properties p ){
        Object ret = internalMap.get(key);
        if(p == null){
            p = new Properties();
        }
        if (ret == null) {
            return p;
        }
        if(ret instanceof Map){
            Map<String, Object> map = (Map<String, Object>) ret;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                p.setProperty(entry.getKey(), String.valueOf(entry.getValue()));
            }
            return p;
        }else{
            throw new RuntimeException(String.format("cant't %s from %s to map, internalMap = %s", key, ret.getClass().getName(), GsonUtil.GSON.toJson(internalMap)));
        }
    }

}
