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

import java.util.HashMap;
import java.util.Map;

/**
 * @author jiangbo
 * @explanation
 * @date 2019/3/25
 */
public class RestoreConfig extends AbstractConfig {

    private static final String KEY_IS_RESTORE = "isRestore";
    private static final String KEY_IS_STREAM = "isStream";
    private static final String KEY_RESTORE_COLUMN_NAME = "restoreColumnName";
    private static final String KEY_RESTORE_COLUMN_TYPE = "restoreColumnType";
    private static final String KEY_RESTORE_COLUMN_INDEX = "restoreColumnIndex";
    private static final String KEY_MAX_ROW_NUM_FOR_CHECKPOINT = "maxRowNumForCheckpoint";

    public RestoreConfig(Map<String, Object> map) {
        super(map);
    }

    public static RestoreConfig defaultConfig(){
        Map<String, Object> map = new HashMap<>(1);
        map.put(KEY_IS_RESTORE, false);
        return new RestoreConfig(map);
    }

    public void configStream(){
        setBooleanVal(KEY_IS_STREAM, true);
    }

    public boolean isRestore(){
        return getBooleanVal(KEY_IS_RESTORE, false);
    }

    public boolean isStream(){
        return isRestore() && getBooleanVal(KEY_IS_STREAM, false);
    }

    public int getRestoreColumnIndex(){
        return getIntVal(KEY_RESTORE_COLUMN_INDEX, -1);
    }

    public void setRestoreColumnIndex(int index){
        setIntVal(KEY_RESTORE_COLUMN_INDEX, index);
    }

    public void setRestoreColumnType(String type){
        setStringVal(KEY_RESTORE_COLUMN_TYPE, type);
    }

    public String getRestoreColumnType(){
        return getStringVal(KEY_RESTORE_COLUMN_TYPE);
    }

    public String getRestoreColumnName(){
        return getStringVal(KEY_RESTORE_COLUMN_NAME);
    }

    public long getMaxRowNumForCheckpoint(){
        return getLongVal(KEY_MAX_ROW_NUM_FOR_CHECKPOINT, 10000);
    }
}
