package kbaserelationengine.system;

import java.util.Map;

import kbaserelationengine.common.ObjectJsonPath;

public class ObjectTypeParsingRules {
    private String globalObjectType;
    private String storageType;
    private String storageObjectType;
    private String innerSubType;
    private ObjectJsonPath pathToSubObjects;
    private Map<ObjectJsonPath, IndexingRules> indexingPathToRules;
    private ObjectJsonPath primaryKeyPath;
    private Map<ObjectJsonPath, KeyLookupRules> foreignKeyPathToLookupRules;
    
    public String getGlobalObjectType() {
        return globalObjectType;
    }
    
    public void setGlobalObjectType(String globalObjectType) {
        this.globalObjectType = globalObjectType;
    }
    
    public String getStorageType() {
        return storageType;
    }
    
    public void setStorageType(String storageType) {
        this.storageType = storageType;
    }
    
    public String getStorageObjectType() {
        return storageObjectType;
    }
    
    public void setStorageObjectType(String storageObjectType) {
        this.storageObjectType = storageObjectType;
    }
    
    public String getInnerSubType() {
        return innerSubType;
    }
    
    public void setInnerSubType(String innerSubType) {
        this.innerSubType = innerSubType;
    }
    
    public ObjectJsonPath getPathToSubObjects() {
        return pathToSubObjects;
    }
    
    public void setPathToSubObjects(ObjectJsonPath pathToSubObjects) {
        this.pathToSubObjects = pathToSubObjects;
    }

    public Map<ObjectJsonPath, IndexingRules> getIndexingPathToRules() {
        return indexingPathToRules;
    }
    
    public void setIndexingPathToRules(
            Map<ObjectJsonPath, IndexingRules> indexingPathToRules) {
        this.indexingPathToRules = indexingPathToRules;
    }
    
    public ObjectJsonPath getPrimaryKeyPath() {
        return primaryKeyPath;
    }
    
    public void setPrimaryKeyPath(ObjectJsonPath primaryKeyPath) {
        this.primaryKeyPath = primaryKeyPath;
    }
    
    public Map<ObjectJsonPath, KeyLookupRules> getForeignKeyPathToLookupRules() {
        return foreignKeyPathToLookupRules;
    }
    
    public void setForeignKeyPathToLookupRules(
            Map<ObjectJsonPath, KeyLookupRules> foreignKeyPathToLookupRules) {
        this.foreignKeyPathToLookupRules = foreignKeyPathToLookupRules;
    }
}
