package com.dtstack.flinkx.restapi.common.exception;

public class ResponseBreakException extends RuntimeException{
    public ResponseBreakException() {
    }

    public ResponseBreakException(String message) {
        super(message);
    }

    public ResponseBreakException(String message, Throwable cause) {
        super(message, cause);
    }

    public ResponseBreakException(Throwable cause) {
        super(cause);
    }

    public ResponseBreakException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
