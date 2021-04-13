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
package com.dtstack.flinkx.connector.mysql.inputFormat;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import com.dtstack.flinkx.connector.jdbc.inputFormat.JdbcInputFormat;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.commons.collections.CollectionUtils;

import java.io.IOException;

/**
 * Date: 2021/04/12
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class MysqlInputFormat extends JdbcInputFormat {

    @Override
    public RowData nextRecordInternal(RowData rowData) throws IOException {
        if (!hasNext) {
            return null;
        }
        GenericRowData genericRowData = new GenericRowData(columnCount);

        try {
            for (int pos = 0; pos < genericRowData.getArity(); pos++) {
                Object obj = resultSet.getObject(pos + 1);
                if(obj != null) {
                    if(CollectionUtils.isNotEmpty(columnTypeList)) {
                        String columnType = columnTypeList.get(pos);
                        if("year".equalsIgnoreCase(columnType)) {
                            java.util.Date date = (java.util.Date) obj;
                            obj = DateUtil.dateToYearString(date);
                        } else if("tinyint".equalsIgnoreCase(columnType)
                                || "bit".equalsIgnoreCase(columnType)) {
                            if(obj instanceof Boolean) {
                                obj = ((Boolean) obj ? 1 : 0);
                            }
                        }
                    }
                    obj = JdbcUtil.clobToString(obj);
                }

                //todo 临时这样写
                if(obj instanceof String){
                    obj = StringData.fromString((String)obj);
                }

                genericRowData.setField(pos, obj);
            }
            return super.nextRecordInternal(genericRowData);
        }catch (Exception e) {
            throw new IOException("Couldn't read data - " + e.getMessage(), e);
        }
    }
}
