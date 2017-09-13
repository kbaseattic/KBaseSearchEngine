package kbasesearchengine.parse;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import kbasesearchengine.system.RelationRules;

public class SimpleIdConsumer extends IdConsumer {
    private Object primaryKey = null;
    private Map<RelationRules, Set<Object>> rulesToForeignKeys = null;
    
    public Object getPrimaryKey() {
        return primaryKey;
    }
    
    public Map<RelationRules, Set<Object>> getRulesToForeignKeys() {
        return rulesToForeignKeys;
    }
    
    @Override
    public void setPrimaryId(Object value) {
        this.primaryKey = value;
    }
    
    @Override
    public void addForeignKeyId(RelationRules lookupRules, Object value) {
        if (rulesToForeignKeys == null) {
            rulesToForeignKeys = new LinkedHashMap<>();
        }
        Set<Object> set = rulesToForeignKeys.get(lookupRules);
        if (set == null) {
            set = new LinkedHashSet<>();
            rulesToForeignKeys.put(lookupRules, set);
        }
        set.add(value);
    }
}
