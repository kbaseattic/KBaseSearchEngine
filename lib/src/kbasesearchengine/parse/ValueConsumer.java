package kbasesearchengine.parse;

import kbasesearchengine.events.exceptions.IndexingException;

public interface ValueConsumer<T> {
    public void addValue(T rules, Object value)
            throws IndexingException, InterruptedException, ObjectParseException;
}
