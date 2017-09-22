package kbasesearchengine.events.exceptions;

@FunctionalInterface
public interface RetryFunction<T, R> {

    /**
     * Applies this function to the given argument.
     *
     * @param t the function argument
     * @return the function result
     */
    R apply(T t) throws IndexingException;
}
