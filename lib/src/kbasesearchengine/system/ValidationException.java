package kbasesearchengine.system;

/** Base exception of all Search Validation "unexpected" problems.
 * 
 * Created by Arfath on 11/15/17.
 */
public class ValidationException extends Exception {

    private static final long serialVersionUID = 1L;

    public ValidationException() {
    }

    public ValidationException(String message) {
        super(message);
    }

    public ValidationException(Throwable cause) {
        super(cause);
    }

    public ValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
