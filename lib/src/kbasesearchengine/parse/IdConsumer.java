package kbasesearchengine.parse;

import kbasesearchengine.system.RelationRules;

public abstract class IdConsumer implements ValueConsumer<IdMappingRules> {
    public abstract void setPrimaryId(Object value);
    public abstract void addForeignKeyId(RelationRules lookupRules, Object value);
    
    @Override
    public void addValue(IdMappingRules rules, Object value) {
        if (rules.isPrimaryKey()) {
            setPrimaryId(value);
        } else {
            addForeignKeyId(rules.getForeignKeyRules(), value);
        }
    }
}
