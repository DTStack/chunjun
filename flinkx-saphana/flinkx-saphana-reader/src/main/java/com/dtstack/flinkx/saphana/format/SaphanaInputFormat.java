/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.saphana.format;

import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.types.Row;

import java.io.IOException;

import static com.dtstack.flinkx.rdb.util.DbUtil.clobToString;

/**
 * Company: www.dtstack.com
 *
 * @author wuhui
 */
public class SaphanaInputFormat extends JdbcInputFormat {

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        if (!hasNext) {
            return null;
        }
        row = new Row(columnCount);

        try {
            for (int pos = 0; pos < row.getArity(); pos++) {
                Object obj = resultSet.getObject(pos + 1);
                if(obj != null) {
                    if(CollectionUtils.isNotEmpty(columnTypeList)) {
                        String columnType = columnTypeList.get(pos);
                        if("tinyint".equalsIgnoreCase(columnType)) {
                            if(obj instanceof Boolean) {
                                obj = ((Boolean) obj ? 1 : 0);
                            }
                        }
                    }
                    obj = clobToString(obj);
                }

                row.setField(pos, obj);
            }
            return super.nextRecordInternal(row);
        }catch (Exception e) {
            throw new IOException("Couldn't read data - " + e.getMessage(), e);
        }
    }

}
