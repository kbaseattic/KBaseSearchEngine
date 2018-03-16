package kbasesearchengine.events.exceptions;

import kbasesearchengine.tools.Utils;

/** The root class for all non-retriable indexing exceptions.
 * @author gaprice@lbl.gov
 *
 */
@SuppressWarnings("serial")
public class IndexingException extends Exception {

    private final ErrorType errorType;
    
    protected IndexingException(final ErrorType errorType, final String message) {
        super(message);
        Utils.nonNull(errorType, "errorType");
        this.errorType = errorType;
    }
    
    protected IndexingException(
            final ErrorType errorType,
            final String message,
            final Throwable cause) {
        super(message, cause);
        Utils.nonNull(errorType, "errorType");
        this.errorType = errorType;
    }

    /** Get the type of this error.
     * @return the error type.
     */
    public ErrorType getErrorType() {
        return errorType;
    }
}
