package kbasesearchengine.events.exceptions;

/** A critical indexing exception that necessitates shutting down the indexing loop.
 * For example, the indexer's credentials for a data source are invalid.
 * @author gaprice@lbl.gov
 *
 */
@SuppressWarnings("serial")
public class FatalIndexingException extends IndexingException {

    public FatalIndexingException(final ErrorType errorType, final String message) {
        super(errorType, message);
    }
    
    public FatalIndexingException(
            final ErrorType errorType,
            final String message,
            final Throwable cause) {
        super(errorType, message, cause);
    }
    
}
