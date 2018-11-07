package com.dtstack.flinkx.carbondata.writer;


import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.dtstack.flinkx.rdb.util.DBUtil;
import com.dtstack.flinkx.util.ClassUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;


/**
 * OutputFormat for writing data to carbondata via jdbc
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbonOutputFormat extends RichOutputFormat {

    protected String username;

    protected String password;

    protected String table;

    protected List<String> column;

    protected List<String> columnType = new ArrayList<>();

    private List<String> fullColumn;

    private List<String> fullColumnType;

    private List<String> parts;

    private List<String> partTypes;

    private List<Integer> indices = new ArrayList<>();

    protected String dbURL;

    protected Connection dbConn;

    private static String DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";

    private PreparedStatement singleUpload;

    private PreparedStatement multipleUpload;

    private final static String DATE_REGEX = "(?i)date";

    private final static String TIMESTAMP_REGEX = "(?i)timestamp";

    private String values;

    private int batchSize = 2000;

    protected boolean overwrite = false;

    protected List<String> preSql;

    protected List<String> postSql;

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        try {
            ClassUtil.forName(DRIVER_CLASS, getClass().getClassLoader());
            dbConn = DBUtil.getConnection(dbURL, username, password);

            initDynamicSettings();

            analyzeTable();
            lowcaseList(column);
            lowcaseList(fullColumn);

            for(String col : column) {
                columnType.add(fullColumnType.get(fullColumn.indexOf(col)));
            }

            initColIndex();
            values = makeValues();
            singleUpload = prepareSingleTemplates();
            multipleUpload = prepareMultipleTemplates(batchSize);
            LOG.info("subtask[" + taskNumber + "] wait finished");
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        }
    }

    private void initDynamicSettings() throws SQLException {
        Statement stmt = dbConn.createStatement();
        stmt.execute("set hive.exec.dynamic.partition=true");
        stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict");
        stmt.execute("set hive.exec.max.dynamic.partitions=500000");
        stmt.execute("set hive.exec.max.created.files=150000");
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        int index = 0;
        try {
            for (; index < row.getArity(); index++) {
                String type = columnType.get(indices.get(index));
                fillUploadStmt(singleUpload, index+1, getField(row, indices.get(index)), type);
            }
            singleUpload.execute();
        } catch (Exception e) {
            if(index < row.getArity()) {
                throw new WriteRecordException(recordConvertDetailErrorMessage(index, row), e, index, row);
            }
            throw new WriteRecordException(e.getMessage(), e);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        PreparedStatement upload;
        if(rows.size() == batchInterval) {
            upload = multipleUpload;
        } else {
            upload = prepareMultipleTemplates(rows.size());
        }

        int k = 1;
        for(int i = 0; i < rows.size(); ++i) {
            Row row = rows.get(i);
            for(int j = 0; j < row.getArity(); ++j) {
                String type = columnType.get(indices.get(j));
                fillUploadStmt(upload, k, getField(row, indices.get(j)), type);
                k++;
            }
        }

        upload.execute();
    }

    protected PreparedStatement prepareSingleTemplates() throws SQLException {
        return dbConn.prepareStatement(makeSingleInsertSql());
    }

    protected PreparedStatement prepareMultipleTemplates(int batchSize) throws SQLException {
        return dbConn.prepareStatement(makeMultipleInsertSql(batchSize));
    }

    private String makeSingleInsertSql() {
        return "insert into " + table + " values " + values;
    }

    private String makeMultipleInsertSql(int size) {
        return "insert into " + table + " values " + makeMultipleValues(size);
    }

    private String makeValues () {
        StringBuilder sb = new StringBuilder("(");
        for(int i = 0; i < fullColumn.size(); ++i) {
            if(i != 0) {
                sb.append(",");
            }
            if(column.contains(fullColumn.get(i))) {
                sb.append("?");
            } else {
                sb.append("null");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    private String makeMultipleValues (int n) {
        return StringUtils.repeat(values, ",", n);
    }

    private Object getField(Row row, int index) {
        Object field = row.getField(index);
        if (field != null && field.getClass() == java.util.Date.class) {
            java.util.Date d = (java.util.Date) field;
            field = new Timestamp(d.getTime());
        }
        return field;
    }

    private void fillUploadStmt(PreparedStatement upload, int k, Object field, String type) throws SQLException {
        if(type.matches(DATE_REGEX)) {
            if (field instanceof Timestamp){
                field = new java.sql.Date(((Timestamp) field).getTime());
            }
            upload.setDate(k, (java.sql.Date) field);
        } else if(type.matches(TIMESTAMP_REGEX)) {
            upload.setTimestamp(k, (Timestamp) field);
        } else {
            upload.setObject(k, field);
        }
    }

    private void initColIndex() {
        for(String col : fullColumn) {
            int index = column.indexOf(col);
            if(index != -1) {
                indices.add(index);
            }
        }
    }

    private void analyzeTable() {
        fullColumn = new ArrayList<>();
        fullColumnType = new ArrayList<>();
        parts = new ArrayList<>();
        partTypes = new ArrayList<>();
        try {
            Statement stmt = dbConn.createStatement();
            ResultSet rs = stmt.executeQuery("desc " + table);
            boolean partition = false;
            while(rs.next()) {
                String col = rs.getString(1);
                String type = rs.getString(2);
                if(col == null || col.trim().length() == 0) {
                    continue;
                }
                if(col.startsWith("#")) {
                    partition = true;
                    continue;
                }
                if(partition) {
                    parts.add(col);
                    partTypes.add(type);
                } else {
                    fullColumn.add(col);
                    fullColumnType.add(type);
                }

            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private List<String> lowcaseList(List<String> src) {
        return src.stream().map(elem -> elem.toLowerCase()).collect(Collectors.toList());
    }

    @Override
    public void closeInternal() {
        if(taskNumber != 0) {
            DBUtil.closeDBResources(null,null,dbConn);
            dbConn = null;
        }
    }

    @Override
    protected boolean needWaitBeforeWriteRecords() {
        return  preSql != null && preSql.size() != 0;
    }

    @Override
    protected void beforeWriteRecords()  {
        if(taskNumber == 0) {
            DBUtil.executeOneByOne(dbConn, preSql);
        }
    }

    @Override
    protected boolean needWaitBeforeCloseInternal() {
        return postSql != null && postSql.size() != 0;
    }

    @Override
    protected void beforeCloseInternal() {
        // 执行postsql
        if(taskNumber == 0) {
            DBUtil.executeOneByOne(dbConn, postSql);
        }
    }
}
