package io.malachai.datafaker.exception;

public class TableNotInitializedException extends RuntimeException {

    public TableNotInitializedException() {
        this("Table not initialized");
    }

    public TableNotInitializedException(String message) {
        super(message);
    }
}
