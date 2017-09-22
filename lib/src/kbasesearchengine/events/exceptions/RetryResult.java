package kbasesearchengine.events.exceptions;

public class RetryResult<T> {
    
    private final T result;
    private final IndexingException exception;
    
    public RetryResult(final T result) {
        this.result = result;
        this.exception = null;
    }
    
    public RetryResult(final IndexingException e) {
        this.result = null;
        this.exception = e;
    }
    
    public boolean hasException() {
        return exception != null;
    }

    public T getResult() {
        return result;
    }

    public IndexingException getException() {
        return exception;
    }
}
