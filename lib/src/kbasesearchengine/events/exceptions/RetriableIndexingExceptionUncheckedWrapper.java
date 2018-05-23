package kbasesearchengine.events.exceptions;

import kbasesearchengine.tools.Utils;

/** This class wraps a checked retriable indexing exception. It exists solely to allow throwing
 * indexing exceptions within interface implementations where the appropriate exception cannot be
 * thrown.
 * 
 * Exception handlers should catch the exception and handle the appropriate instance of the wrapped
 * checked exception.
 * 
 * @author gaprice@lbl.gov
 *
 */
@SuppressWarnings("serial")
public class RetriableIndexingExceptionUncheckedWrapper extends RuntimeException {

    public RetriableIndexingExceptionUncheckedWrapper(final RetriableIndexingException cause) {
        super(checkNonNull(cause));
    }

    private static RetriableIndexingException checkNonNull(
            final RetriableIndexingException cause) {
        Utils.nonNull(cause, "cause");

        return cause;
    }
    
    public RetriableIndexingException getIndexingException() {
        return (RetriableIndexingException) getCause();
    }
    
}
