package kbasesearchengine.events.exceptions;

/** A critical indexing exception that necessitates shutting down the indexing loop if retrying
 * does not solve the issue.
 * For example, a temporary file cannot be created.
 * @author gaprice@lbl.gov
 *
 */
@SuppressWarnings("serial")
public class FatalRetriableIndexingException extends RetriableIndexingException {

    public FatalRetriableIndexingException(final ErrorType errorType, final String message) {
        super(errorType, message);
    }
    
    public FatalRetriableIndexingException(
            final ErrorType errorType,
            final String message,
            final Throwable cause) {
        super(errorType, message, cause);
    }
    
}
