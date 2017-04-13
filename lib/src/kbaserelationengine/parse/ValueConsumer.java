package kbaserelationengine.parse;

public interface ValueConsumer<T> {
    public void addValue(T rules, Object value);
}
