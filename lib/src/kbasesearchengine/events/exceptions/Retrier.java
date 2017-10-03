package kbasesearchengine.events.exceptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;

import kbasesearchengine.events.ObjectStatusEvent;
import kbasesearchengine.tools.Utils;

/** Generic code for retrying functions. Expects the code to throw
 * {@link RetriableIndexingException} or a subclass when a retry should occur.
 * @author gaprice@lbl.gov
 *
 */
public class Retrier {
    
    //TODO TEST
    
    private final int retryCount;
    private final int delayMS;
    private final RetryLogger logger;
    private final List<Integer> fatalRetryBackoffsMS;
    
    /** Create a retrier.
     * @param retryCount the maximum number of retries for non-fatal exceptions.
     * @param delayMS the millisecond delay between retries for non-fatal exceptions.
     * @param fatalRetryBackoffsMS the number of milliseconds to wait between each retry for
     * fatal exceptions, in order. The number of entries in the list determine the number of
     * retries. If it is empty, no retries will occur.
     * @param logger a logger to which retries will be logged.
     */
    public Retrier(
            final int retryCount,
            final int delayMS,
            final List<Integer> fatalRetryBackoffsMS,
            final RetryLogger logger) {
        if (retryCount < 1) {
            throw new IllegalArgumentException("retryCount must be at least 1");
        }
        if (delayMS < 1) {
            throw new IllegalArgumentException("delayMS must be at least 1");
        }
        Utils.nonNull(fatalRetryBackoffsMS, "fatalRetryBackoffsMS");
        for (final Integer i: fatalRetryBackoffsMS) {
            if (i == null || i < 1) {
                throw new IllegalArgumentException("Illegal value in fatalRetryBackoffsMS: " + i);
            }
        }
        Utils.nonNull(logger, "logger");
        this.retryCount = retryCount;
        this.delayMS = delayMS;
        this.fatalRetryBackoffsMS = Collections.unmodifiableList(
                new ArrayList<>(fatalRetryBackoffsMS));
        this.logger = logger;
    }

    /** Get the maximum number of retries for non-fatal exceptions.
     * @return the retry count.
     */
    public int getRetryCount() {
        return retryCount;
    }

    /** Get the delay in milliseconds between non-fatal retry attempts.
     * @return the retry delay.
     */
    public int getDelayMS() {
        return delayMS;
    }
    
    /** Get the delays between subsequent retries for fatal but retriable events.
     * @return the retry delays.
     */
    public List<Integer> getFatalRetryBackoffsMS() {
        return fatalRetryBackoffsMS;
    }
    
    /** Get the logger for logging retries.
     * @return the logger.
     */
    public RetryLogger getLogger() {
        return logger;
    }
    
    /** Retry a "function" that only takes one input.
     * @param consumer the consumer function.
     * @param input the input to the function.
     * @param event the event associated with the function or null if none.
     * @throws InterruptedException if the retry is interrupted.
     * @throws IndexingException if an exception that cannot be retried occurs or retries are
     * expended.
     */
    public <T> void retryCons(
            final RetryConsumer<T> consumer,
            final T input,
            final ObjectStatusEvent event)
            throws InterruptedException, IndexingException {
        Utils.nonNull(consumer, "consumer");
        int retries = 1;
        int fatalRetries = 1;
        while (true) {
            try {
                consumer.accept(input);
                return;
            } catch (RetriableIndexingException e) {
                final boolean fatal = handleException(event, e, retries, fatalRetries);
                if (fatal) {
                    fatalRetries++;
                } else {
                    retries++;
                }
            }
        }
    }
    
    /** Retry a function that only takes one input.
     * @param function the function.
     * @param input the input to the function.
     * @param event the event associated with the function or null if none.
     * @throws InterruptedException if the retry is interrupted.
     * @throws IndexingException if an exception that cannot be retried occurs or retries are
     * expended.
     */
    public <T, R> R retryFunc(
            final RetryFunction<T, R> function,
            final T input,
            final ObjectStatusEvent event)
            throws InterruptedException, IndexingException {
        Utils.nonNull(function, "function");
        int retries = 1;
        int fatalRetries = 1;
        while (true) {
            try {
                return function.apply(input);
            } catch (RetriableIndexingException e) {
                final boolean fatal = handleException(event, e, retries, fatalRetries);
                if (fatal) {
                    fatalRetries++;
                } else {
                    retries++;
                }
            }
        }
    }
    
    private boolean handleException(
            final ObjectStatusEvent event,
            final RetriableIndexingException e,
            final int retries,
            final int fatalRetries)
            throws InterruptedException, IndexingException {
        if (e instanceof FatalRetriableIndexingException) {
            if (fatalRetries - 1 >= fatalRetryBackoffsMS.size()) {
                throw new FatalIndexingException(e.getMessage(), e);
            } else {
                logger.log(fatalRetries, Optional.fromNullable(event), e);
                TimeUnit.MILLISECONDS.sleep(fatalRetryBackoffsMS.get(fatalRetries - 1));
                return true;
            }
        } else if (e instanceof RetriableIndexingException){
            if (retries >= retryCount) {
                throw new RetriesExceededIndexingException(e.getMessage(), e);
            } else {
                logger.log(retries, Optional.fromNullable(event), e);
                TimeUnit.MILLISECONDS.sleep(delayMS);
                return false;
            }
        } else {
            throw new IllegalStateException(
                    "Exception hierarchy changed without update to retrier");
        }
        
    }

}
