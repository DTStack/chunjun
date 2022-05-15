package com.dtstack.chunjun.connector.oracle.source;

import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;

import java.sql.Timestamp;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class OracleInputFormat extends JdbcInputFormat {

    /**
     * 构建时间边界字符串
     *
     * @param location 边界位置(起始/结束)
     * @return
     */
    @Override
    protected String getTimeStr(Long location) {
        String timeStr;
        Timestamp ts = new Timestamp(JdbcUtil.getMillis(location));
        ts.setNanos(JdbcUtil.getNanos(location));
        timeStr = JdbcUtil.getNanosTimeStr(ts.toString());
        timeStr = timeStr.substring(0, 23);
        timeStr = String.format("TO_TIMESTAMP('%s','yyyy-MM-dd HH24:mi:ss.FF6')", timeStr);

        return timeStr;
    }
}
