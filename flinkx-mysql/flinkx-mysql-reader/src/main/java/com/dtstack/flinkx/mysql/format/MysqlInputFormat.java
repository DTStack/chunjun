package com.dtstack.flinkx.mysql.format;

import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import com.dtstack.flinkx.rdb.util.DBUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import static com.dtstack.flinkx.rdb.util.DBUtil.clobToString;

/**
 * Date: 2019/09/19
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class MysqlInputFormat extends JdbcInputFormat {


    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        try {
            LOG.info(inputSplit.toString());

            ClassUtil.forName(drivername, getClass().getClassLoader());

            if (incrementConfig.isIncrement() && incrementConfig.isUseMaxFunc()){
                getMaxValue(inputSplit);
            }

            initMetric(inputSplit);

            if(!canReadData(inputSplit)){
                LOG.warn("Not read data when the start location are equal to end location");

                hasNext = false;
                return;
            }

            dbConn = DBUtil.getConnection(dbURL, username, password);

            // 部分驱动需要关闭事务自动提交，fetchSize参数才会起作用
            dbConn.setAutoCommit(false);

            Statement statement = dbConn.createStatement(resultSetType, resultSetConcurrency);

            statement.setFetchSize(Integer.MIN_VALUE);

            statement.setQueryTimeout(queryTimeOut);
            String querySql = buildQuerySql(inputSplit);
            resultSet = statement.executeQuery(querySql);
            columnCount = resultSet.getMetaData().getColumnCount();

            boolean splitWithRowCol = numPartitions > 1 && StringUtils.isNotEmpty(splitKey) && splitKey.contains("(");
            if(splitWithRowCol){
                columnCount = columnCount-1;
            }

            hasNext = resultSet.next();

            if (StringUtils.isEmpty(customSql)){
                descColumnTypeList = DBUtil.analyzeTable(dbURL, username, password,databaseInterface,table,metaColumns);
            } else {
                descColumnTypeList = new ArrayList<>();
                for (MetaColumn metaColumn : metaColumns) {
                    descColumnTypeList.add(metaColumn.getName());
                }
            }

        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed. " + se.getMessage(), se);
        }

        LOG.info("JdbcInputFormat[" + jobName + "]open: end");
    }

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
                    if(descColumnTypeList != null && descColumnTypeList.size() != 0) {
                        if(descColumnTypeList.get(pos).equalsIgnoreCase("year")) {
                            java.util.Date date = (java.util.Date) obj;
                            obj = DateUtil.dateToYearString(date);
                        } else if(descColumnTypeList.get(pos).equalsIgnoreCase("tinyint")) {
                            if(obj instanceof Boolean) {
                                obj = ((Boolean) obj ? 1 : 0);
                            }
                        } else if(descColumnTypeList.get(pos).equalsIgnoreCase("bit")) {
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
