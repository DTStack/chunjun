package com.dtstack.flinkx.postgresql.writer;

import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormat;
import com.google.common.base.Strings;
import org.apache.flink.types.Row;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * when  postgresql with mode insert, it use 'copy tableName(columnName) from stdin' syntax
 * Date: 2019/8/5
 * Company: www.dtstack.com
 * @author xuchao
 */

public class PostgresqlOutputFormat extends JdbcOutputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresqlOutputFormat.class);

    private static final String COPY_SQL_TEMPL = "copy %s(%s) from stdin DELIMITER '%s'";

    private static final String DEFAULT_FIELD_DELIM = "\001";

    private static final String LINE_DELIMITER = "\n";

    /**now just add ext insert mode:copy*/
    private static final String INSERT_SQL_MODE_TYPE = "copy";

    private String copySql = "";

    private CopyManager copyManager;


    @Override
    protected PreparedStatement prepareTemplates() throws SQLException {
        if(fullColumn == null || fullColumn.size() == 0) {
            fullColumn = column;
        }

        //check is use copy mode for insert
        if (EWriteMode.INSERT.name().equalsIgnoreCase(mode) && checkIsCopyMode(insertSqlMode)) {
            copyManager = new CopyManager((BaseConnection) dbConn);
            copySql = String.format(COPY_SQL_TEMPL, table, String.join(",", column), DEFAULT_FIELD_DELIM);
            return null;
        }

        return super.prepareTemplates();
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        if(!checkIsCopyMode(insertSqlMode)){
            super.writeSingleRecordInternal(row);
            return;
        }

        //write with copy
        int index = 0;
        try {
            StringBuilder sb = new StringBuilder();
            for (; index < row.getArity(); index++) {
                Object rowData = getField(row, index);
                sb.append(rowData)
                        .append(DEFAULT_FIELD_DELIM);
            }

            String rowVal = sb.toString();
            ByteArrayInputStream bi = new ByteArrayInputStream(rowVal.getBytes());
            copyManager.copyIn(copySql, bi);
        } catch (Exception e) {
            if(index < row.getArity()) {
                throw new WriteRecordException(recordConvertDetailErrorMessage(index, row), e, index, row);
            }
            throw new WriteRecordException(e.getMessage(), e);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        if(!checkIsCopyMode(insertSqlMode)){
            super.writeMultipleRecordsInternal();
            return;
        }

        StringBuilder sb = new StringBuilder();
        for (Row row : rows) {
            int lastIndex = row.getArity() - 1;
            for (int index =0; index < row.getArity(); index++) {
                Object rowData = getField(row, index);
                sb.append(rowData);
                if(index != lastIndex){
                    sb.append(DEFAULT_FIELD_DELIM);
                }
            }

            sb.append(LINE_DELIMITER);
        }

        String rowVal = sb.toString();
        ByteArrayInputStream bi = new ByteArrayInputStream(rowVal.getBytes());
        copyManager.copyIn(copySql, bi);
    }

    private boolean checkIsCopyMode(String insertMode){
        if(Strings.isNullOrEmpty(insertMode)){
            return false;
        }

        if(!INSERT_SQL_MODE_TYPE.equalsIgnoreCase(insertMode)){
            throw new RuntimeException("not support insertSqlMode:" + insertMode);
        }

        return true;
    }


}
