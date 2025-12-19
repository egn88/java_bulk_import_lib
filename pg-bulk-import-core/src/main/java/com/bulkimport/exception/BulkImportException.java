package com.bulkimport.exception;

/**
 * Base exception for all bulk import operations.
 * All exceptions thrown by this library extend this class.
 */
public class BulkImportException extends RuntimeException {

    public BulkImportException(String message) {
        super(message);
    }

    public BulkImportException(String message, Throwable cause) {
        super(message, cause);
    }
}
