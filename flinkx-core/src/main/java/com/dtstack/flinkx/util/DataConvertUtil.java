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


package com.dtstack.flinkx.util;

import com.dtstack.flinkx.enums.ColumnType;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.Date;

/**
 * @author jiangbo
 * @date 2019/7/23
 */
public class DataConvertUtil {

    public static Long toLong(String columnType, Object val){
        Long longVal;
        if (val == null){
            return null;
        }

        if (ColumnType.isTimeType(columnType)){
            String timeStr;
            if(val instanceof Timestamp){
                long time = ((Timestamp)val).getTime() / 1000;

                String nanosStr = String.valueOf(((Timestamp)val).getNanos());
                if(nanosStr.length() == 9){
                    timeStr = time + nanosStr;
                } else {
                    String fillZeroStr = StringUtils.repeat("0",9 - nanosStr.length());
                    timeStr = time + fillZeroStr + nanosStr;
                }
            } else {
                Date date = DateUtil.stringToDate(val.toString(),null);
                String fillZeroStr = StringUtils.repeat("0",6);
                long time = date.getTime();
                timeStr = time + fillZeroStr;
            }

            longVal = Long.valueOf(timeStr);
        } else if(ColumnType.isNumberType(columnType)){
            longVal = Long.valueOf(val.toString());
        } else {
            longVal = Long.valueOf(val.toString());
        }

        return longVal;
    }
}
