package exceptions;

/**
 * ApplicationConfigException should be raised when an app config
 * error occurs such as a bad xpath query. When an ApplicationConfigException
 * is thrown, no error emails are sent.
 */
public class ApplicationConfigException extends RuntimeException {
    public ApplicationConfigException(String message) {
        super(message);
    }
}
