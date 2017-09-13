package kbasesearchengine.parse;

import java.io.IOException;

public interface ValueConsumer<T> {
    public void addValue(T rules, Object value) throws IOException;
}
