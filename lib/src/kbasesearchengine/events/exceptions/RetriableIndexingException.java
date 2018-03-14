package kbasesearchengine.events.exceptions;

import kbasesearchengine.tools.Utils;

/** An exception thrown when a particular event could not be processed, but a retry is possible.
 * Generally speaking, a handler should try again with a delay and fail hard after
 * some number of retries. For example, a data source might not be contactable temporarily.
 * @author gaprice@lbl.gov
 *
 */
@SuppressWarnings("serial")
public class RetriableIndexingException extends Exception {
    
    private final ErrorType errorType;

    public RetriableIndexingException(final ErrorType errorType, final String message) {
        super(message);
        Utils.nonNull(errorType, "errorType");
        this.errorType = errorType;
    }
    
    public RetriableIndexingException(
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
