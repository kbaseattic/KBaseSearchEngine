package kbasesearchengine.events.exceptions;

import kbasesearchengine.events.ObjectStatusEvent;

/** A logging interface for the retrier.
 * @see Retrier
 * @author gaprice@lbl.gov
 *
 */
public interface RetryLogger {
    
    /** Log a retry.
     * @param retryCount the number of retries so far.
     * @param event an event associated with the retry. May be null.
     * @param e the exception that occurred on the current retry.
     */
    void log(int retryCount, ObjectStatusEvent event, RetriableIndexingException e);

}
