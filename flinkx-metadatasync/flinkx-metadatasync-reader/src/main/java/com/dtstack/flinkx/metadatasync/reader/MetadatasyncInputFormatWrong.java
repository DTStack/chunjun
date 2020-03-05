package com.dtstack.flinkx.metadatasync.reader;

import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import com.dtstack.flinkx.util.ClassUtil;
import org.apache.flink.core.io.InputSplit;

import java.io.IOException;

/**
 * @author : tiezhu
 * @date : 2020/3/4
 * @description :
 */
public class MetadatasyncInputFormatWrong extends JdbcInputFormat {

    protected String sql = "";

    @Override
    public void closeInternal() throws IOException {

    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        try {
            LOG.info(inputSplit.toString());

            ClassUtil.forName(drivername, getClass().getClassLoader());

            executeQuery(buildDescSql(table, inputSplit, true));

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private String buildDescSql(String table, InputSplit split, boolean formatted) {
        if (split == null) {
            LOG.warn("inputSplit = null, Executing sql is: '{}", sql);
        }
        if (formatted) {
            sql = String.format("DESC FORMATTED " + table);
        } else {
            sql = String.format("DESC ", table);
        }
        return sql;
    }
}
