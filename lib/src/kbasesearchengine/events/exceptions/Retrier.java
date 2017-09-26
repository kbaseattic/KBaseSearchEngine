package kbasesearchengine.events.exceptions;

import java.util.ArrayList;
import java.util.List;

import kbasesearchengine.events.ObjectStatusEvent;

public class Retrier {
    
    //TODO JAVADOC
    //TODO TEST
    
    private final int retryCount;
    private final int delayMS;
    private final RetryLogger logger;
    private final List<Integer> fatalRetryBackoffsMS;
    
    public Retrier(
            final int retryCount,
            final int delayMS,
            final List<Integer> fatalRetryBackoffsMS,
            final RetryLogger logger) {
        this.retryCount = retryCount;
        this.delayMS = delayMS;
        this.fatalRetryBackoffsMS = new ArrayList<>(fatalRetryBackoffsMS);
        this.logger = logger;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public int getDelayMS() {
        return delayMS;
    }
    
    public <T> void retryCons(
            final RetryConsumer<T> consumer,
            final T input,
            final ObjectStatusEvent event)
            throws InterruptedException, IndexingException {
        int retries = 1;
        int fatalRetries = 1;
        while (true) {
            try {
                consumer.accept(input);
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
    
    public <T, R> R retryFunc(
            final RetryFunction<T, R> function,
            final T input,
            final ObjectStatusEvent event)
            throws InterruptedException, IndexingException {
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
            if (retries - 1 >= fatalRetryBackoffsMS.size()) {
                throw new FatalIndexingException(e.getMessage(), e);
            } else {
                logger.log(retries, event, e);
                Thread.sleep(fatalRetryBackoffsMS.get(fatalRetries - 1));
                return true;
            }
        } else if (e instanceof RetriableIndexingException){
            if (retries > retryCount) {
                throw new RetriesExceededIndexingException(e.getMessage(), e);
            } else {
                logger.log(retries, event, e);
                Thread.sleep(delayMS);
                return false;
            }
        } else {
            throw new IllegalStateException("Exception hierarchy changed without update to retrier");
        }
        
    }

}
