package kbaserelationengine.parse;

public interface IdConsumer {
    public void setPrimaryId(Object value);
    public void addForeignKeyId(KeyLookupRules lookupRules, Object value);
}
