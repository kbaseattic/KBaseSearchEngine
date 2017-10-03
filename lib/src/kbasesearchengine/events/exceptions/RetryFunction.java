package kbasesearchengine.events.exceptions;

/** A retriable function.
 * @author gaprice@lbl.gov
 *
 * @param <T> The input type for the function.
 * @param <R> the return type of the function.
 */
@FunctionalInterface
public interface RetryFunction<T, R> {

    /**
     * Applies this function to the given argument.
     *
     * @param t the function argument.
     * @return the function result.
     * @throws IndexingException if a non-retriable indexing exception occurred.
     * @throws RetriableIndexingException if a retriable indexing exception occurred.
     * @throws InterruptedException if the function is interrupted.
     */
    R apply(T t) throws IndexingException, RetriableIndexingException, InterruptedException;
}
