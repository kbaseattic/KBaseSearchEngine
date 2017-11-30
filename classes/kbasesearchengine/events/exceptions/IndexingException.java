package kbasesearchengine.events.exceptions;

/** The root class for all non-retriable indexing exceptions.
 * @author gaprice@lbl.gov
 *
 */
@SuppressWarnings("serial")
public class IndexingException extends Exception {

    protected IndexingException(final String message) {
        super(message);
    }
    
    protected IndexingException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
}
