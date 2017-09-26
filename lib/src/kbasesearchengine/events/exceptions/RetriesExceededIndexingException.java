package kbasesearchengine.events.exceptions;

/** An exception thrown when a particular event could not be processed, but a retry is possible.
 * Generally speaking, a handler should try again with an increasing delay and fail hard after
 * some number of retries. For example, a data source might not be contactable temporarily.
 * @author gaprice@lbl.gov
 *
 */
@SuppressWarnings("serial")
public class RetriesExceededIndexingException extends IndexingException {

    public RetriesExceededIndexingException(final String message) {
        super(message);
    }
    
    public RetriesExceededIndexingException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
}
