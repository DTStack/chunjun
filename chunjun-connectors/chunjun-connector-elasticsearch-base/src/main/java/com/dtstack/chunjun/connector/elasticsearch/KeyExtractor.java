package com.dtstack.chunjun.connector.elasticsearch;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KeyExtractor {
    /**
     * generate doc id by id fields.
     *
     * @param
     * @return
     */
    public static String getDocId(
            List<String> idFieldNames, Map<String, Object> dataMap, String keyDelimiter) {
        String docId = "";
        if (null != idFieldNames) {
            docId =
                    idFieldNames.stream()
                            .map(idFiledName -> dataMap.get(idFiledName).toString())
                            .collect(Collectors.joining(keyDelimiter));
        }
        return docId;
    }
}
