package com.dtstack.flinkx.carbondata.writer.dict;


import org.apache.carbondata.core.metadata.ColumnIdentifier;

import java.util.List;


/**
 * Dictionary related detail
 */
public class DictionaryDetail {

    ColumnIdentifier[] columnIdentifiers;

    String[] dictFilePaths;

    Boolean[] dictFileExists;

    public DictionaryDetail(ColumnIdentifier[] columnIdentifiers, String[] dictFilePaths, Boolean[] dictFileExists) {
        this.columnIdentifiers = columnIdentifiers;
        this.dictFilePaths = dictFilePaths;
        this.dictFileExists = dictFileExists;
    }

}
