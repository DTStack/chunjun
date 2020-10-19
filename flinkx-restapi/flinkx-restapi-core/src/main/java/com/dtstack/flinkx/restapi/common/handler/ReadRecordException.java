package com.dtstack.flinkx.restapi.common.handler;

public class ReadRecordException extends RuntimeException {
    public ReadRecordException() {
    }

    public ReadRecordException(String message) {
        super(message);
    }
}
