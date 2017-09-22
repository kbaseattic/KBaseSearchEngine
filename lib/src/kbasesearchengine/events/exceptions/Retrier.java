package kbasesearchengine.events.exceptions;

import kbasesearchengine.events.ObjectStatusEvent;

public class Retrier {
    
    private final int retryCount;
    private final int delayMS;
    private final RetryLogger logger;
    
    public Retrier(final int retryCount, final int delayMS, final RetryLogger logger) {
        this.retryCount = retryCount;
        this.delayMS = delayMS;
        this.logger = logger;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public int getDelayMS() {
        return delayMS;
    }
    
    public <T> RetryResult<Void> retryCons(
            final RetryConsumer<T> consumer,
            final T input,
            final ObjectStatusEvent event)
            throws InterruptedException {
        int retries = 1;
        while (true) {
            try {
                consumer.accept(input);
                return new RetryResult<>((Void) null);
            } catch (IndexingException e) {
                final RetryResult<Void> res = handleException(event, e, retries);
                if (res != null) {
                    return res;
                }
                retries++;
            }
            Thread.sleep(delayMS);
        }
    }
    
    public <T, R> RetryResult<R> retryFunc(
            final RetryFunction<T, R> function,
            final T input,
            final ObjectStatusEvent event)
            throws InterruptedException {
        int retries = 1;
        while (true) {
            try {
                return new RetryResult<>(function.apply(input));
            } catch (IndexingException e) {
                final RetryResult<R> res = handleException(event, e, retries);
                if (res != null) {
                    return res;
                }
                retries++;
            }
            Thread.sleep(delayMS);
        }
    }
    
    private <R> RetryResult<R> handleException(
            final ObjectStatusEvent event,
            final IndexingException e,
            final int retries) {
        if (e instanceof RetriableIndexingException) {
            if (retries > retryCount) {
                return new RetryResult<>(e);
            } else {
                logger.log(retries, event, e);
                return null;
            }
        } else {
            return new RetryResult<>(e);
        }
        
    }

}
