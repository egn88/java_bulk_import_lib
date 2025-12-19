package com.bulkimport.exception;

/**
 * Exception thrown when the bulk import configuration is invalid.
 */
public class ConfigurationException extends BulkImportException {

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates an exception for missing required configuration.
     */
    public static ConfigurationException missingRequired(String configName) {
        return new ConfigurationException(
            String.format("Missing required configuration: '%s'", configName)
        );
    }

    /**
     * Creates an exception for missing conflict columns when using UPDATE_SPECIFIED strategy.
     */
    public static ConfigurationException missingConflictColumns() {
        return new ConfigurationException(
            "Conflict columns must be specified when using UPDATE_SPECIFIED or UPDATE_ALL conflict strategy"
        );
    }

    /**
     * Creates an exception for missing update columns when using UPDATE_SPECIFIED strategy.
     */
    public static ConfigurationException missingUpdateColumns() {
        return new ConfigurationException(
            "Update columns must be specified when using UPDATE_SPECIFIED conflict strategy"
        );
    }
}
