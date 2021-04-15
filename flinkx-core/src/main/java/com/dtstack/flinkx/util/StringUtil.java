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

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.exception.WriteRecordException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * String Utilities
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class StringUtil {

    public static final int STEP_SIZE = 2;

    /**
     * Handle the escaped escape charactor.
     *
     * e.g. Turnning \\t into \t, etc.
     *
     * @param str The String to convert
     * @return the converted String
     */
    public static String convertRegularExpr (String str) {
        if(str == null){
            return "";
        }

        String pattern = "\\\\(\\d{3})";

        Pattern r = Pattern.compile(pattern);
        while(true) {
            Matcher m = r.matcher(str);
            if(!m.find()) {
                break;
            }
            String num = m.group(1);
            int x = Integer.parseInt(num, 8);
            str = m.replaceFirst(String.valueOf((char)x));
        }
        str = str.replaceAll("\\\\t","\t");
        str = str.replaceAll("\\\\r","\r");
        str = str.replaceAll("\\\\n","\n");

        return str;
    }

    public static Object string2col(String str, String type, SimpleDateFormat customTimeFormat) {
        if(str == null || str.length() == 0 || type == null){
            return str;
        }

        ColumnType columnType = ColumnType.getType(type.toUpperCase());
        Object ret;
        switch(columnType) {
            case TINYINT:
                ret = Byte.valueOf(str.trim());
                break;
            case SMALLINT:
                ret = Short.valueOf(str.trim());
                break;
            case INT:
                ret = Integer.valueOf(str.trim());
                break;
            case MEDIUMINT:
            case BIGINT:
            case LONG:
                ret = Long.valueOf(str.trim());
                break;
            case FLOAT:
                ret = Float.valueOf(str.trim());
                break;
            case DOUBLE:
                ret = Double.valueOf(str.trim());
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                if(customTimeFormat != null){
                    ret = DateUtil.columnToDate(str,customTimeFormat);
                    ret = DateUtil.timestampToString((Date)ret);
                } else {
                    ret = str;
                }
                break;
            case BOOLEAN:
                ret = Boolean.valueOf(str.trim().toLowerCase());
                break;
            case DATE:
                ret = DateUtil.columnToDate(str,customTimeFormat);
                break;
            case TIMESTAMP:
            case DATETIME:
                ret = DateUtil.columnToTimestamp(str,customTimeFormat);
                break;
            default:
                ret = str;
        }

        return ret;
    }

    public static String col2string(Object column, String type) {
        if(column == null){
            return "";
        }

        if(type == null){
            return column.toString();
        }

        String rowData = column.toString();
        ColumnType columnType = ColumnType.getType(type.toUpperCase());
        Object result;
        switch (columnType) {
            case TINYINT:
                result = Byte.valueOf(rowData.trim());
                break;
            case SMALLINT:
            case SHORT:
                result = Short.valueOf(rowData.trim());
                break;
            case INT:
            case INTEGER:
                result = Integer.valueOf(rowData.trim());
                break;
            case BIGINT:
            case LONG:
                if (column instanceof Timestamp){
                    result=((Timestamp) column).getTime();
                }else {
                    result = Long.valueOf(rowData.trim());
                }
                break;
            case FLOAT:
                result = Float.valueOf(rowData.trim());
                break;
            case DOUBLE:
                result = Double.valueOf(rowData.trim());
                break;
            case DECIMAL:
                result = new BigDecimal(rowData.trim());
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
            case TEXT:
                if (column instanceof Timestamp){
                    result = DateUtil.timestampToString((java.util.Date)column);
                }else {
                    result = rowData;
                }
                break;
            case BOOLEAN:
                result = Boolean.valueOf(rowData.trim());
                break;
            case DATE:
                result = DateUtil.dateToString(DateUtil.columnToDate(column, null));
                break;
            case DATETIME:
            case TIMESTAMP:
                result = DateUtil.timestampToString(DateUtil.columnToTimestamp(column, null));
                break;
            default:
                result = rowData;
        }
        return result.toString();
    }


    public static String row2string(Row row, List<String> columnTypes, String delimiter) throws WriteRecordException {
        // convert row to string
        int cnt = row.getArity();
        StringBuilder sb = new StringBuilder(128);

        int i = 0;
        try {
            for (; i < cnt; ++i) {
                if (i != 0) {
                    sb.append(delimiter);
                }

                Object column = row.getField(i);

                if(column == null) {
                    continue;
                }

                sb.append(col2string(column, columnTypes.get(i)));
            }
        } catch(Exception ex) {
            String msg = "StringUtil.row2string error: when converting field[" + i + "] in Row(" + row + ")";
            throw new WriteRecordException(msg, ex, i, row);
        }

        return sb.toString();
    }

    public static byte[] hexStringToByteArray(String hexString) {
        if (hexString == null) {
            return null;
        }

        int length = hexString.length();

        byte[] bytes = new byte[length / 2];
        for (int i = 0; i < length; i += STEP_SIZE) {
            bytes[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                    + Character.digit(hexString.charAt(i+1), 16));
        }

        return bytes;
    }

    /**
     * Split the specified string delimiter --- ignored quotes delimiter
     * @param str 待解析字符串,不考虑分割结果需要带'[',']','\"','\''的情况
     * @param delimiter 分隔符
     * @return 分割后的字符串数组
     * Example: "[dbo_test].[table]" => "[dbo_test, table]"
     * Example: "[dbo.test].[table.test]" => "[dbo.test, table.test]"
     * Example: "[dbo.test].[[[tab[l]e]]" => "[dbo.test, table]"
     * Example："[\"dbo_test\"].[table]" => "[dbo_test, table]"
     * Example:"['dbo_test'].[table]" => "[dbo_test, table]"
     */
    public static List<String> splitIgnoreQuota(String str, char delimiter){
        List<String> tokensList = new ArrayList<>();
        boolean inQuotes = false;
        boolean inSingleQuotes = false;
        int bracketLeftNum = 0;
        StringBuilder b = new StringBuilder(64);
        char[] chars = str.toCharArray();
        int idx = 0;
        for (char c : chars) {
            char flag = 0;
            if (idx > 0) {
                flag = chars[idx - 1];
            }
            if (c == delimiter) {
                if (inQuotes) {
                    b.append(c);
                } else if (inSingleQuotes) {
                    b.append(c);
                } else if (bracketLeftNum > 0) {
                    b.append(c);
                } else {
                    tokensList.add(b.toString());
                    b = new StringBuilder();
                }
            } else if (c == '\"' && '\\' != flag && !inSingleQuotes) {
                inQuotes = !inQuotes;
                //b.append(c);
            } else if (c == '\'' && '\\' != flag && !inQuotes) {
                inSingleQuotes = !inSingleQuotes;
                //b.append(c);
            } else if (c == '[' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum++;
                //b.append(c);
            } else if (c == ']' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum--;
                //b.append(c);
            } else {
                b.append(c);
            }
            idx++;
        }

        tokensList.add(b.toString());

        return tokensList;
    }

    /**
     * 字符串转换成对应时间戳字符串
     * @param location
     * @return
     */
    public static String stringToTimestampStr(String location, ColumnType type){
        //若为空字符串或本身就是时间戳则不需要转换
        if(StringUtils.isBlank(location) || StringUtils.isNumeric(location)){
            return location;
        }
        try {
            switch (type) {
                case TIMESTAMP: return String.valueOf(Timestamp.valueOf(location).getTime());
                case DATE: return String.valueOf(DateUtils.parseDate(location, DateUtil.getDateFormat(location)).getTime());
                default: return location;
            }
        }catch (ParseException e){
            String message = String.format("cannot transform 【%s】to 【%s】, e = %s", location, type, ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(message);
        }
    }

    /**
     * 调用{@linkplain com.dtstack.flinkx.util.StringUtil}的splitIgnoreQuota处理 并对返回结果按照.拼接
     * @param table [dbo.schema1].[table]
     * @return dbo.schema1.table
     */
    public static String splitIgnoreQuotaAndJoinByPoint(String table) {
        List<String> strings = StringUtil.splitIgnoreQuota(table, ConstantValue.POINT_SYMBOL.charAt(0));
        StringBuffer stringBuffer = new StringBuffer(64);
        for(int i =0; i < strings.size(); i++){
            stringBuffer.append(strings.get(i));
            if(i != strings.size()-1){
                stringBuffer.append(ConstantValue.POINT_SYMBOL);
            }
        }
        return stringBuffer.toString();
    }


    /**
     * 转义正则特殊字符 （$()*+.[]?\^{},|）
     *
     * @param keyword 需要转义特殊字符串的文本
     * @return 特殊字符串转义后的文本
     */
    public static String escapeExprSpecialWord(String keyword) {
        if (StringUtils.isNotBlank(keyword)) {
            String[] fbsArr = {"\\", "$", "(", ")", "*", "+", ".", "[", "]", "?", "^", "{", "}", "|"};
            for (String key : fbsArr) {
                if (keyword.contains(key)) {
                    keyword = keyword.replace(key, "\\" + key);
                }
            }
        }
        return keyword;
    }
}
