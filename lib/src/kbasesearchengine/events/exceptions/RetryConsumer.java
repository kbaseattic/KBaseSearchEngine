package kbasesearchengine.events.exceptions;

import java.util.function.Consumer;

@FunctionalInterface
public interface RetryConsumer<T> {

    /**
     * Performs this operation on the given argument.
     *
     * @param t the input argument
     * @see Consumer
     */
    void accept(T t) throws IndexingException, RetriableIndexingException, InterruptedException;
}
