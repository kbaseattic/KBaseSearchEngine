package kbasesearchengine.events.exceptions;

/** An exception thrown when a particular event cannot be processed. For example, a data unit
 * may have been deleted between the generation of the event at the data source and the processing
 * of the event at the indexer.
 * @author gaprice@lbl.gov
 *
 */
@SuppressWarnings("serial")
public class UnprocessableEventIndexingException extends IndexingException {

    public UnprocessableEventIndexingException(final ErrorType errorType, final String message) {
        super(errorType, message);
    }
    
    public UnprocessableEventIndexingException(
            final ErrorType errorType,
            final String message,
            final Throwable cause) {
        super(errorType, message, cause);
    }
    
}
