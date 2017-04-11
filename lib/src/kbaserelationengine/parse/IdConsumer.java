package kbaserelationengine.parse;

import kbaserelationengine.system.RelationRules;

public interface IdConsumer {
    public void setPrimaryId(Object value);
    public void addForeignKeyId(RelationRules lookupRules, Object value);
}
