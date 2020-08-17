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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The configuration of job config
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class JobConfig extends AbstractConfig {

    public static final String KEY_SETTING_CONFIG = "setting";
    public static final String KEY_CONTENT_CONFIG_LIST = "content";
    public final static String KEY_READER_CONFIG = "reader";
    public final static String KEY_WRITER_CONFIG = "writer";
    public final static String KEY_PARAMETERS = "parameter";
    public static final String KEY_PASSWORD = "password";
    public static final String KEY_CONFUSE_PASSWORD = "******";
    private SettingConfig setting;
    private List<ContentConfig> content;

    protected final Logger LOG = LoggerFactory.getLogger(getClass());


    @SuppressWarnings("unchecked")
    public JobConfig(Map<String, Object> map) {
        super(map);
        setting = new SettingConfig((Map<String, Object>) map.get(KEY_SETTING_CONFIG));
        content = new ArrayList<>();
        if(map.containsKey(KEY_CONTENT_CONFIG_LIST)) {
            List<Map<String,Object>> contentList = (List<Map<String, Object>>) map.get(KEY_CONTENT_CONFIG_LIST);
            for(Map<String,Object> contentMap : contentList) {
                content.add(new ContentConfig(contentMap));
                //隐藏密码信息
                Map<String, Object> readerConfig = (Map<String, Object>)contentMap.get(KEY_READER_CONFIG);
                Map<String, Object> readerParameter = (Map<String, Object>)readerConfig.get(KEY_PARAMETERS);
                if(readerParameter.containsKey(KEY_PASSWORD)){
                    readerParameter.put(KEY_PASSWORD, KEY_CONFUSE_PASSWORD);
                }
                Map<String, Object> writerConfig = (Map<String, Object>)contentMap.get(KEY_WRITER_CONFIG);
                Map<String, Object> writerParameter = (Map<String, Object>)writerConfig.get(KEY_PARAMETERS);
                if(writerParameter.containsKey(KEY_PASSWORD)){
                    writerParameter.put(KEY_PASSWORD, KEY_CONFUSE_PASSWORD);
                }
            }
        }
        LOG.info("configInfo : {}", GsonUtil.GSON.toJson(map));
    }

    public SettingConfig getSetting() {
        return setting;
    }

    public void setSetting(SettingConfig setting) {
        this.setting = setting;
    }

    public List<ContentConfig> getContent() {
        return content;
    }

    public void setContent(List<ContentConfig> content) {
        this.content = content;
    }
}
