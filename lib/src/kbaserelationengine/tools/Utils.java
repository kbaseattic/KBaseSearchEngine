package kbaserelationengine.tools;


import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
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
    
    /** Throws an IllegalArgumentException if the string is null or consists only of whitespace.
     * @param s the string in question.
     * @param message the message for the exception.
     * @throws IllegalArgumentException if the string is null or empty.
     */
    public static void notNullOrEmpty(
            final String s,
            final String message) {
        if (s == null || s.trim().isEmpty()) {
            throw new IllegalArgumentException(message);
        }
    }

    /** Returns true if the string is null or whitespace only.
     * @param s the string in question.
     * @return returns true if the string is null or empty, false otherwise.
     */
    public static boolean isNullOrEmpty(final String s) {
        return s == null || s.trim().isEmpty();
    }

    public static String jsonToPretty(Object obj) throws IOException {
        return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(obj);
    }

}
