package com.dtstack.chunjun.connector.doris.rest;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2022/1/17
 */
public class Carrier implements Serializable {
    private static final long serialVersionUID = 1L;
    private final StringJoiner insertContent;
    private final StringJoiner deleteContent;
    private final String fieldDelimiter;
    private int batch = 0;
    private String database;
    private String table;
    private List<String> columns;
    private final Set<Integer> rowDataIndexes = new HashSet<>();

    public Carrier(String fieldDelimiter, String lineDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
        insertContent = new StringJoiner(lineDelimiter);
        deleteContent = new StringJoiner(" OR ");
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getInsertContent() {
        return insertContent.toString();
    }

    public String getDeleteContent() {
        return deleteContent.toString();
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public Set<Integer> getRowDataIndexes() {
        return rowDataIndexes;
    }

    public void addRowDataIndex(int index) {
        rowDataIndexes.add(index);
    }

    public void addInsertContent(List<String> insertV) {
        if (!insertV.isEmpty()) {
            if (insertV.size() > columns.size()) {
                // It is certain that in this case, the size
                // of insertV is twice the size of column
                List<String> forward = insertV.subList(0, columns.size());
                String forwardV = StringUtils.join(forward, fieldDelimiter);
                insertContent.add(forwardV);
                List<String> behind = insertV.subList(columns.size(), insertV.size());
                String behindV = StringUtils.join(behind, fieldDelimiter);
                insertContent.add(behindV);
            } else {
                String s = StringUtils.join(insertV, fieldDelimiter);
                insertContent.add(s);
            }
        }
    }

    public void addDeleteContent(List<String> deleteV) {
        if (!deleteV.isEmpty()) {
            String s = buildMergeOnConditions(columns, deleteV);
            deleteContent.add(s);
        }
    }

    public void updateBatch() {
        batch++;
    }

    /**
     * Construct the Doris delete on condition, which only takes effect in the merge http request.
     *
     * @return delete on condition
     */
    private String buildMergeOnConditions(List<String> columns, List<String> values) {
        List<String> deleteOnStr = new ArrayList<>();
        for (int i = 0, size = columns.size(); i < size; i++) {
            String s =
                    columns.get(i)
                            + "<=>"
                            + "'"
                            + ((values.get(i)) == null ? "" : values.get(i))
                            + "'";
            deleteOnStr.add(s);
        }
        return StringUtils.join(deleteOnStr, " AND ");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{database:");
        sb.append(database);
        sb.append(", table:");
        sb.append(table);
        sb.append(", columns:[");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(columns.get(i));
        }
        sb.append("], insert_value:");
        sb.append(insertContent);
        sb.append(", delete_value:");
        sb.append(deleteContent);
        sb.append(", batch:");
        sb.append(batch);
        sb.append("}");
        return sb.toString();
    }
}
