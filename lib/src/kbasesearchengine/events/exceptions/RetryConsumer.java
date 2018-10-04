package kbasesearchengine.events.exceptions;

import kbasesearchengine.search.IndexingConflictException;

import java.io.IOException;

/** A retriable "function" that returns no value.
 * @author gaprice@lbl.gov
 *
 * @param <T> the input type for the consumer.
 */
@FunctionalInterface
public interface RetryConsumer<T> {

    /**
     * Performs this operation on the given argument.
     *
     * @param t the input argument.
     * @throws IndexingException if a non-retriable indexing exception occurred.
     * @throws RetriableIndexingException if a retriable indexing exception occurred.
     * @throws InterruptedException if the function is interrupted.
     * @see Consumer
     */
    void accept(T t) throws IndexingException, RetriableIndexingException, InterruptedException, IOException, IndexingConflictException;
}
