package kbaserelationengine.tools;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Utils {

    public static void nonNull(final Object o, final String message) {
        if (o == null) {
            throw new NullPointerException(message);
        }
    }

    public static String jsonToPretty(Object obj) throws IOException {
        return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(obj);
    }
}
