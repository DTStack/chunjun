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

package com.dtstack.chunjun.util;

import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.enums.ColumnType;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {

    public static final int STEP_SIZE = 2;

    public static final char[] hexChars = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    /**
     * Handle the escaped escape charactor.
     *
     * <p>e.g. Turnning \\t into \t, etc.
     *
     * @param str The String to convert
     * @return the converted String
     */
    public static String convertRegularExpr(String str) {
        if (str == null) {
            return "";
        }

        String pattern = "\\\\(\\d{3})";

        Pattern r = Pattern.compile(pattern);
        while (true) {
            Matcher m = r.matcher(str);
            if (!m.find()) {
                break;
            }
            String num = m.group(1);
            int x = Integer.parseInt(num, 8);
            str = m.replaceFirst(String.valueOf((char) x));
        }
        str = str.replaceAll("\\\\t", "\t");
        str = str.replaceAll("\\\\r", "\r");
        str = str.replaceAll("\\\\n", "\n");

        return str;
    }

    // TODO 类型可以改成使用LogicalType
    public static Object string2col(String str, String type, SimpleDateFormat customTimeFormat) {
        if (str == null || str.length() == 0 || type == null) {
            return str;
        }

        ColumnType columnType = ColumnType.getType(type.toUpperCase());
        Object ret;
        switch (columnType) {
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
                if (customTimeFormat != null) {
                    ret = DateUtil.columnToDate(str, customTimeFormat);
                    ret = DateUtil.timestampToString((Date) ret);
                } else {
                    ret = str;
                }
                break;
            case BOOLEAN:
                ret = Boolean.valueOf(str.trim().toLowerCase());
                break;
            case DATE:
                ret = DateUtil.columnToDate(str, customTimeFormat);
                break;
            case TIMESTAMP:
            case DATETIME:
                ret = DateUtil.columnToTimestamp(str, customTimeFormat);
                break;
            case OBJECT:
                ret = GsonUtil.GSON.fromJson(str, Map.class);
                break;

            default:
                ret = str;
        }

        return ret;
    }

    public static String col2string(Object column, String type) {
        if (column == null) {
            return "";
        }

        if (type == null) {
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
                if (column instanceof Timestamp) {
                    result = ((Timestamp) column).getTime();
                } else {
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
                if (column instanceof Timestamp) {
                    result = DateUtil.timestampToString((java.util.Date) column);
                } else {
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

    public static String row2string(RowData rowData, List<String> columnTypes, String delimiter)
            throws WriteRecordException {
        // convert rowData to string
        int cnt = rowData.getArity();
        StringBuilder sb = new StringBuilder(128);

        int i = 0;
        try {
            for (; i < cnt; ++i) {
                if (i != 0) {
                    sb.append(delimiter);
                }

                Object column = ((GenericRowData) rowData).getField(i);

                if (column == null) {
                    continue;
                }

                sb.append(col2string(column, columnTypes.get(i)));
            }
        } catch (Exception ex) {
            String msg =
                    "StringUtil.row2string error: when converting field["
                            + i
                            + "] in Row("
                            + rowData
                            + ")";
            throw new WriteRecordException(msg, ex, i, rowData);
        }

        return sb.toString();
    }

    /**
     * 16进制数组 转为hex字符串
     *
     * @param b
     * @return
     */
    public static String bytesToHexString(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte value : b) {
            int hexVal = value & 0xFF;
            sb.append(hexChars[(hexVal & 0xF0) >> 4]);
            sb.append(hexChars[(hexVal & 0x0F)]);
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
            bytes[i / 2] =
                    (byte)
                            ((Character.digit(hexString.charAt(i), 16) << 4)
                                    + Character.digit(hexString.charAt(i + 1), 16));
        }

        return bytes;
    }

    /**
     * Split the specified string delimiter --- ignored quotes delimiter
     *
     * @param str 待解析字符串,不考虑分割结果需要带'[',']','\"','\''的情况
     * @param delimiter 分隔符
     * @return 分割后的字符串数组 Example: "[dbo_test].[table]" => "[dbo_test, table]" Example:
     *     "[dbo.test].[table.test]" => "[dbo.test, table.test]" Example: "[dbo.test].[[[tab[l]e]]"
     *     => "[dbo.test, table]" Example："[\"dbo_test\"].[table]" => "[dbo_test, table]"
     *     Example:"['dbo_test'].[table]" => "[dbo_test, table]"
     */
    public static List<String> splitIgnoreQuota(String str, char delimiter) {
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
                // b.append(c);
            } else if (c == '\'' && '\\' != flag && !inQuotes) {
                inSingleQuotes = !inSingleQuotes;
                // b.append(c);
            } else if (c == '[' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum++;
                // b.append(c);
            } else if (c == ']' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum--;
                // b.append(c);
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
     *
     * @param location
     * @return
     */
    public static String stringToTimestampStr(String location, ColumnType type) {
        // 若为空字符串或本身就是时间戳则不需要转换
        if (StringUtils.isBlank(location) || StringUtils.isNumeric(location)) {
            return location;
        }
        try {
            switch (type) {
                case TIMESTAMP:
                case DATETIME:
                case TIMESTAMPTZ:
                    return String.valueOf(Timestamp.valueOf(location).getTime());
                case DATE:
                    return String.valueOf(
                            DateUtils.parseDate(location, DateUtil.getDateFormat(location))
                                    .getTime());
                default:
                    return location;
            }
        } catch (ParseException e) {
            String message = String.format("cannot transform 【%s】to 【%s】", location, type);
            throw new ChunJunRuntimeException(message, e);
        }
    }

    /**
     * 调用{@linkplain com.dtstack.chunjun.util.StringUtil}的splitIgnoreQuota处理 并对返回结果按照.拼接
     *
     * @param table [dbo.schema1].[table]
     * @return dbo.schema1.table
     */
    public static String splitIgnoreQuotaAndJoinByPoint(String table) {
        List<String> strings =
                StringUtil.splitIgnoreQuota(table, ConstantValue.POINT_SYMBOL.charAt(0));
        StringBuffer stringBuffer = new StringBuffer(64);
        for (int i = 0; i < strings.size(); i++) {
            stringBuffer.append(strings.get(i));
            if (i != strings.size() - 1) {
                stringBuffer.append(ConstantValue.POINT_SYMBOL);
            }
        }
        return stringBuffer.toString();
    }

    /**
     * get String from inputStream
     *
     * @param input inputStream
     * @return String value
     * @throws IOException convert exception
     */
    public static String inputStream2String(InputStream input) throws IOException {
        StringBuilder stringBuffer = new StringBuilder();
        byte[] byt = new byte[1024];
        for (int i; (i = input.read(byt)) != -1; ) {
            stringBuffer.append(new String(byt, 0, i));
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
            String[] fbsArr = {
                "\\", "$", "(", ")", "*", "+", ".", "[", "]", "?", "^", "{", "}", "|"
            };
            for (String key : fbsArr) {
                if (keyword.contains(key)) {
                    keyword = keyword.replace(key, "\\" + key);
                }
            }
        }
        return keyword;
    }

    /**
     * Serialize properties to string.
     *
     * @param props properties
     * @return Serialized properties
     * @throws IllegalArgumentException error
     */
    public static String propsToString(Properties props) throws IllegalArgumentException {
        StringWriter sw = new StringWriter();
        if (props != null) {
            try {
                props.store(sw, "");
            } catch (IOException ex) {
                throw new IllegalArgumentException("Cannot parse props to String.", ex);
            }
        }
        return sw.toString();
    }

    /**
     * add line Number after new line
     *
     * @param str
     * @return
     */
    public static String addLineNumber(String str) {
        String[] lines = StringUtils.splitByWholeSeparatorPreserveAllTokens(str, "\n");
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= lines.length; i++) {
            int length = String.valueOf(i).length();
            switch (length) {
                case 1:
                    sb.append(i).append(">    ");
                    break;
                case 2:
                    sb.append(i).append(">   ");
                    break;
                case 3:
                    sb.append(i).append(">  ");
                    break;
                default:
                    sb.append(i).append("> ");
                    break;
            }
            sb.append(lines[i - 1]).append("\n");
        }
        return sb.toString();
    }

    /**
     * 处理 sql 中 "--" 注释，而不删除引号内的内容
     *
     * @param sql 解析出来的 sql
     * @return 返回无注释内容的 sql
     */
    public static String dealSqlComment(String sql) {
        boolean inQuotes = false;
        boolean inSingleQuotes = false;
        StringBuilder b = new StringBuilder(sql.length());
        char[] chars = sql.toCharArray();
        for (int index = 0; index < chars.length; index++) {
            StringBuilder tempSb = new StringBuilder(2);
            if (index >= 1) {
                tempSb.append(chars[index - 1]);
                tempSb.append(chars[index]);
            }

            if ("--".equals(tempSb.toString())) {
                if (inQuotes) {
                    b.append(chars[index]);
                } else if (inSingleQuotes) {
                    b.append(chars[index]);
                } else {
                    b.deleteCharAt(b.length() - 1);
                    while (chars[index] != '\n') {
                        // 判断注释内容是不是行尾或者 sql 的最后一行
                        if (index == chars.length - 1) {
                            break;
                        }
                        index++;
                    }
                }
            } else if (chars[index] == '\"' && '\\' != chars[index] && !inSingleQuotes) {
                inQuotes = !inQuotes;
                b.append(chars[index]);
            } else if (chars[index] == '\'' && '\\' != chars[index] && !inQuotes) {
                inSingleQuotes = !inSingleQuotes;
                b.append(chars[index]);
            } else {
                b.append(chars[index]);
            }
        }
        return b.toString();
    }

    /**
     * Capitalize the first letter. The ascii encoding of the letters is moved forward, and the
     * efficiency is higher than the operation of intercepting the string for conversion
     *
     * @param str the string which the first letter should be capitalized.
     * @return the string which the first letter has been capitalized.
     */
    public static String captureFirstLetter(String str) {
        char[] cs = str.toCharArray();
        if (cs[0] > 32) {
            cs[0] -= 32;
        }
        return String.valueOf(cs);
    }
}
