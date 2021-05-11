package com.dtstack.flinkx.oracle9.format;

import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.Timestamp;

import static com.dtstack.flinkx.rdb.util.DbUtil.clobToString;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/30 17:22
 */
public class Oracle9InputFormat extends JdbcInputFormat {
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
                    if((obj instanceof java.util.Date
                            || obj.getClass().getSimpleName().toUpperCase().contains("TIMESTAMP")) ) {
                        obj = resultSet.getTimestamp(pos + 1);
                    }
                    obj = clobToString(obj);
                    //XMLType transform to String
                    obj = xmlTypeToString(obj);
                }
                row.setField(pos, obj);
            }
            return super.nextRecordInternal(row);
        }catch (Exception e) {
            throw new IOException("Couldn't read data - " + e.getMessage(), e);
        }
    }

    /**
     * 构建时间边界字符串
     * @param location          边界位置(起始/结束)
     * @param incrementColType  增量字段类型
     * @return
     */
    @Override
    protected String getTimeStr(Long location, String incrementColType){
        String timeStr;
        Timestamp ts = new Timestamp(DbUtil.getMillis(location));
        ts.setNanos(DbUtil.getNanos(location));
        timeStr = DbUtil.getNanosTimeStr(ts.toString());

        if(ColumnType.TIMESTAMP.name().equals(incrementColType)){
            //纳秒精度为9位
            timeStr = String.format("TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS:FF9')", timeStr);
        } else {
            timeStr = timeStr.substring(0, 19);
            timeStr = String.format("TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')", timeStr);
        }

        return timeStr;
    }

    /**
     * XMLType to String
     * @param obj xmltype
     * @return
     * @throws Exception
     */
    public Object xmlTypeToString(Object obj) throws Exception{
        String dataStr;
        if(obj instanceof oracle.xdb.XMLType){
            oracle.xdb.XMLType xml = (oracle.xdb.XMLType)obj;
            BufferedReader bf = new BufferedReader(xml.getCharacterStream());
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = bf.readLine()) != null){
                stringBuilder.append(line);
            }
            dataStr = stringBuilder.toString();
        } else {
            return obj;
        }

        return dataStr;
    }

}
