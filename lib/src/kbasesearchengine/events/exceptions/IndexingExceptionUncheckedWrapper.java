package kbasesearchengine.events.exceptions;

import kbasesearchengine.tools.Utils;

/** This class wraps a checked indexing exception. It exists solely to allow throwing indexing
 * exceptions within interface implementations where the appropriate exception cannot be thrown.
 * 
 * Exception handlers should catch the exception and handle the appropriate instance of the wrapped
 * checked exception.
 * 
 * @author gaprice@lbl.gov
 *
 */
@SuppressWarnings("serial")
public class IndexingExceptionUncheckedWrapper extends RuntimeException {
    
    public IndexingExceptionUncheckedWrapper(final IndexingException cause) {
        super(checkNonNull(cause));
    }

    private static IndexingException checkNonNull(final IndexingException cause) {
        Utils.nonNull(cause, "cause");

        return cause;
    }
    
    public IndexingException getIndexingException() {
        return (IndexingException) getCause();
    }
    

}
