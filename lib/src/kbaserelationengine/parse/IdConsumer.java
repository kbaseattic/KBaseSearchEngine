package kbaserelationengine.parse;

import kbaserelationengine.system.KeyLookupRules;

public interface IdConsumer {
    public void setPrimaryId(Object value);
    public void addForeignKeyId(KeyLookupRules lookupRules, Object value);
}
