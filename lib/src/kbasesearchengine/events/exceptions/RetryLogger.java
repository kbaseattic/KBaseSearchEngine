package kbasesearchengine.events.exceptions;

import kbasesearchengine.events.ObjectStatusEvent;

public interface RetryLogger {
    
    void log(int retryCount, ObjectStatusEvent event, RetriableIndexingException e);

}
