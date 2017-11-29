package kbasesearchengine.events.exceptions;

import com.google.common.base.Optional;

import kbasesearchengine.events.StatusEventWithId;

/** A logging interface for the retrier.
 * @see Retrier
 * @author gaprice@lbl.gov
 *
 */
public interface RetryLogger {
    
    /** Log a retry.
     * @param retryCount the number of retries so far.
     * @param event an event associated with the retry. May be absent.
     * @param e the exception that occurred on the current retry.
     */
    void log(int retryCount, Optional<StatusEventWithId> event, RetriableIndexingException e);

}
