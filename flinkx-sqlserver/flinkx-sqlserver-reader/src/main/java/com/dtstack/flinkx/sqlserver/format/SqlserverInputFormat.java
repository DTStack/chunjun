package com.dtstack.flinkx.sqlserver.format;

import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import org.apache.flink.types.Row;

import java.io.IOException;

import static com.dtstack.flinkx.rdb.util.DBUtil.clobToString;

/**
 * Date: 2019/09/19
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class SqlserverInputFormat extends JdbcInputFormat {

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
                        if(descColumnTypeList.get(pos).equalsIgnoreCase("bit")) {
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
