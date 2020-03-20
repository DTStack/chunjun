/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.metadata.inputformat;

import com.dtstack.flinkx.inputformat.RichInputFormatBuilder;

import java.util.List;
import java.util.Map;

/**
 * @author : tiezhu
 * @date : 2020/3/8
 */
public class MetadataInputFormatBuilder extends RichInputFormatBuilder {
    private MetadataInputFormat format;

    public MetadataInputFormatBuilder(MetadataInputFormat format) {
        super.format = this.format = format;
    }

    public void setDBUrl(String dbUrl) {
        format.dbUrl = dbUrl;
    }

    public void setUsername(String username) {
        format.username = username;
    }

    public void setPassword(String password) {
        format.password = password;
    }

    public void setNumPartitions(int numPartitions) {
        format.numPartitions = numPartitions;
    }

    public void setDriverName(String driverName) {
        format.driverName = driverName;
    }

    public void setDBList(List<Map> dbList){
        format.dbList = dbList;
    }

    @Override
    protected void checkFormat() {
        if (format.password == null || format.username == null) {
            throw new IllegalArgumentException("请检查用户密码是否填写");
        }
        if (format.dbUrl == null) {
            throw new IllegalArgumentException("请检查url是否填写");
        }
        // 判断是否是全库全表查询
        if(format.dbList.isEmpty()){
            format.isAll = true;
        } else {
            format.isAll = false;
        }
    }


}
