package kbaserelationengine.tools;

import java.util.Collection;

public class Utils {

    /** Throws a null pointer exception if any elements in a collection are null.
     * @param col the collection to check.
     * @param message the exception message.
     * @param <T> the type of the elements in the collection.
     */
    public static <T> void noNulls(final Collection<T> col, final String message) {
        for (final T item: col) {
            if (item == null) {
                throw new NullPointerException(message);
            }
        }
    }
    
    /** Throws a null pointer exception if an object is null.
     * @param o the object to check.
     * @param message the message for the exception.
     */
    public static void nonNull(final Object o, final String message) {
        if (o == null) {
            throw new NullPointerException(message);
        }
    }
}
