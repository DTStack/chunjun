package com.dtstack.flinkx.restapi.common.exception;

public class ResponseRetryException extends RuntimeException {
    public ResponseRetryException() {
    }

    public ResponseRetryException(String message) {
        super(message);
    }

    public ResponseRetryException(String message, Throwable cause) {
        super(message, cause);
    }

    public ResponseRetryException(Throwable cause) {
        super(cause);
    }

    public ResponseRetryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
