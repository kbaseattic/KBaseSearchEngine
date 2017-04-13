package kbaserelationengine.parse;

import kbaserelationengine.system.RelationRules;

public class IdMappingRules {
    private boolean isPrimaryKey;
    private RelationRules foreignKeyRules;
    
    public IdMappingRules() {}
    
    public IdMappingRules(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }
    
    public IdMappingRules(RelationRules foreignKeyRules) {
        this.foreignKeyRules = foreignKeyRules;
    }
    
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }
    
    public void setPrimaryKey(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }
    
    public RelationRules getForeignKeyRules() {
        return foreignKeyRules;
    }
    
    public void setForeignKeyRules(RelationRules foreignKeyRules) {
        this.foreignKeyRules = foreignKeyRules;
    }
}
