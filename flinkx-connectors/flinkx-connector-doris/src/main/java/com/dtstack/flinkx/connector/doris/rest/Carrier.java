package com.dtstack.flinkx.connector.doris.rest;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
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
    private final int batchSize;
    private int batch = 0;
    private List<String> columns;

    public Carrier(String fieldDelimiter, String lineDelimiter, int batchSize) {
        this.fieldDelimiter = fieldDelimiter;
        this.batchSize = batchSize;
        insertContent = new StringJoiner(lineDelimiter);
        deleteContent = new StringJoiner(" OR ");
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

    public boolean isFull() {
        return batch >= batchSize;
    }

    /**
     * Construct the Doris delete on condition, which only takes effect in the merge http request.
     *
     * @return delete on condition
     */
    private String buildMergeOnConditions(List<String> columns, List<String> values) {
        List<String> deleteOnStr = new ArrayList<>();
        for (int i = 0, size = columns.size(); i < size; i++) {
            String stringBuilder =
                    columns.get(i)
                            + "<=>"
                            + "'"
                            + ((values.get(i)) == null ? "" : values.get(i))
                            + "'";
            String s =
                    stringBuilder
                            .replaceAll("\\r", "\\\\r")
                            .replaceAll("\\n", "\\\\n")
                            .replaceAll("\\t", "\\\t");
            deleteOnStr.add(s);
        }
        return StringUtils.join(deleteOnStr, " AND ");
    }
}
