package com.dtstack.flinkx.connector.ftp.client;

import java.util.Locale;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2021/11/1
 */
public enum FileType {
    /** File types currently supported by ftp. */
    TXT,
    CSV,
    EXCEL;

    public static FileType fromString(String type) {
        if (type == null) {
            throw new RuntimeException("null FileType!");
        }
        type = type.toUpperCase(Locale.ENGLISH);
        return valueOf(type);
    }
}
