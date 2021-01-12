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
package com.dtstack.flinkx.phoenix5.reader;

import com.dtstack.flinkx.phoenix5.format.Phoenix5InputFormat;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormatBuilder;
import org.apache.commons.lang3.StringUtils;

/**
 * Date: 2020/09/30
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class Phoenix5InputFormatBuilder extends JdbcInputFormatBuilder {
    protected Phoenix5InputFormat format;

    //use for check
    private String where;
    private String customSql;
    private String orderByColumn;

    public Phoenix5InputFormatBuilder(Phoenix5InputFormat format) {
        super(format);
        this.format = format;
    }

    public void setReadFromHbase(boolean readFromHbase){
        format.readFromHbase = readFromHbase;
    }

    public void setScanCacheSize(int scanCacheSize){
        format.scanCacheSize = scanCacheSize;
    }

    public void setScanBatchSize(int scanBatchSize){
        format.scanBatchSize = scanBatchSize;
    }

    @Override
    protected void checkFormat() {
        if(format.readFromHbase){
            StringBuilder sb = new StringBuilder(256);
            if (StringUtils.isBlank(format.dbUrl)) {
                sb.append("No database URL supplied\n");
            }
            if(StringUtils.isNotBlank(where)){
                sb.append("cannot config [where] when readFromHbase is true, current where is : [")
                        .append(where)
                        .append("]\n");
            }
            if(StringUtils.isNotBlank(customSql)){
                sb.append("cannot config [customSql] when readFromHbase is true, current customSql is : [")
                        .append(customSql)
                        .append("]\n");
            }
            if(StringUtils.isNotBlank(orderByColumn)){
                sb.append("cannot config [orderByColumn] when readFromHbase is true, current orderByColumn is : [")
                        .append(orderByColumn)
                        .append("]");
            }
            if(sb.length() > 0){
                throw new IllegalArgumentException(sb.toString());
            }
        }else{
            super.checkFormat();
        }
    }

    public void setWhere(String where) {
        this.where = where;
    }

    @Override
    public void setCustomSql(String customSql) {
        super.setCustomSql(customSql);
        this.customSql = customSql;
    }

    public void setOrderByColumn(String orderByColumn) {
        this.orderByColumn = orderByColumn;
    }
}
