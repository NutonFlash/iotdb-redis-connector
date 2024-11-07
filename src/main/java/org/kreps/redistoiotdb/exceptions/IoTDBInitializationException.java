package org.kreps.redistoiotdb.exceptions;

public class IoTDBInitializationException extends Exception {
    public IoTDBInitializationException(String message) {
        super(message);
    }

    public IoTDBInitializationException(String message, Throwable cause) {
        super(message, cause);
    }
}