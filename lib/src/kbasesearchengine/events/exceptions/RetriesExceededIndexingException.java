package kbasesearchengine.events.exceptions;

/** An exception thrown when a particular event could not be processed and all specified retries
 * have failed.
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
