package kbasesearchengine.events.exceptions;

/** An exception thrown when a particular event could not be processed, but a retry is possible.
 * Generally speaking, a handler should try again with a delay and fail hard after
 * some number of retries. For example, a data source might not be contactable temporarily.
 * @author gaprice@lbl.gov
 *
 */
@SuppressWarnings("serial")
public class RetriableIndexingException extends Exception {

    public RetriableIndexingException(final String message) {
        super(message);
    }
    
    public RetriableIndexingException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
}
