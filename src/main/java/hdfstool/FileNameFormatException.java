package hdfstool;

import java.io.IOException;

/**
 * Exception that indicates a malformed file name.
 */
public class FileNameFormatException extends IOException{
    public FileNameFormatException(String fileName, String message) {
        super(getMessage(fileName, message));
    }

    public FileNameFormatException(String fileName, String message, Throwable cause) {
        super(getMessage(fileName, message), cause);
    }

    private static String getMessage(String fileName, String message) {
        return String.format("Invalid segment file name '%s'. %s", fileName, message);
    }
}
