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

import com.dtstack.flinkx.constants.ConfigConstant;
import com.dtstack.flinkx.util.GsonUtil;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Date: 2021/01/18
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class SinkConf implements Serializable {
    private static final long serialVersionUID = 1L;

    /** sink插件名称 */
    private String name;
    /** sink配置 */
    private Map<String, Object> parameter;
    /** table设置 */
    private TableConf table;

    public List getMetaColumn(){
        return (List) parameter.get(ConfigConstant.KEY_COLUMN);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Object> getParameter() {
        return parameter;
    }

    public void setParameter(Map<String, Object> parameter) {
        this.parameter = parameter;
    }

    public TableConf getTable() {
        return table;
    }

    public void setTable(TableConf table) {
        this.table = table;
    }

    /**
     * 从指定key中获取Properties配置信息
     * @param key
     * @param p
     * @return Properties
     */
    @SuppressWarnings("unchecked")
    public Properties getProperties(String key, Properties p){
        Object ret = parameter.get(key);
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
            throw new RuntimeException(String.format("cant't %s from %s to map, internalMap = %s", key, ret.getClass().getName(), GsonUtil.GSON.toJson(parameter)));
        }
    }


    @Override
    public String toString() {
        return "SinkConf{" +
                "name='" + name + '\'' +
                ", parameter=" + parameter +
                ", table=" + table +
                '}';
    }
}
