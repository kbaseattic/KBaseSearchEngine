package kbaserelationengine.tools;

public class Utils {

    public static void nonNull(final Object o, final String message) {
        if (o == null) {
            throw new NullPointerException(message);
        }
    }
    
}
