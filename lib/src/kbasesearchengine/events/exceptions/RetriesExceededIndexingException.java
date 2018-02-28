package kbasesearchengine.events.exceptions;

/** An exception thrown when a particular event could not be processed and all specified retries
 * have failed.
 * @author gaprice@lbl.gov
 *
 */
@SuppressWarnings("serial")
public class RetriesExceededIndexingException extends IndexingException {

    public RetriesExceededIndexingException(final ErrorType errorType, final String message) {
        super(errorType, message);
    }
    
    public RetriesExceededIndexingException(
            final ErrorType errorType,
            final String message,
            final Throwable cause) {
        super(errorType, message, cause);
    }
    
}
