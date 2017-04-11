package kbaserelationengine.system;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import kbaserelationengine.common.ObjectJsonPath;
import kbaserelationengine.parse.ObjectParseException;
import us.kbase.common.service.UObject;

public class ObjectTypeParsingRules {
    private String globalObjectType;
    private String storageType;
    private String storageObjectType;
    private String innerSubType;
    private ObjectJsonPath pathToSubObjects;
    private Map<ObjectJsonPath, IndexingRules> indexingPathToRules;
    private ObjectJsonPath primaryKeyPath;
    private Map<ObjectJsonPath, RelationRules> relationPathToRules;
    
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
    
    public Map<ObjectJsonPath, RelationRules> getRelationPathToRules() {
        return relationPathToRules;
    }
    
    public void setRelationPathToRules(
            Map<ObjectJsonPath, RelationRules> foreignKeyPathToLookupRules) {
        this.relationPathToRules = foreignKeyPathToLookupRules;
    }
    
    public static ObjectTypeParsingRules fromJson(String json) throws ObjectParseException {
        @SuppressWarnings("unchecked")
        Map<String, Object> obj = UObject.transformStringToObject(json, Map.class);
        return fromObject(obj);
    }

    public static ObjectTypeParsingRules fromStream(InputStream is) 
            throws IOException, ObjectParseException {
        @SuppressWarnings("unchecked")
        Map<String, Object> obj = UObject.getMapper().readValue(is, Map.class);
        return fromObject(obj);
    }

    public static ObjectTypeParsingRules fromObject(Map<String, Object> obj) 
            throws ObjectParseException {
        ObjectTypeParsingRules ret = new ObjectTypeParsingRules();
        ret.setGlobalObjectType((String)obj.get("global-object-type"));
        ret.setStorageType((String)obj.get("storage-type"));
        ret.setStorageObjectType((String)obj.get("storage-object-type"));
        ret.setInnerSubType((String)obj.get("inner-sub-type"));
        ret.setPathToSubObjects(getPath((String)obj.get("path-to-sub-objects")));
        // Indexing
        @SuppressWarnings("unchecked")
        Map<String, Object> indexingPathToRules = 
                (Map<String, Object>)obj.get("indexing-path-to-rules");
        if (indexingPathToRules != null) {
            ret.setIndexingPathToRules(new LinkedHashMap<>());
            for (String key : indexingPathToRules.keySet()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> rulesObj = (Map<String, Object>)indexingPathToRules.get(key);
                IndexingRules rules = new IndexingRules();
                rules.setFullText((Boolean)rulesObj.get("full-text"));
                rules.setKeywordType((String)rulesObj.get("keyword-type"));
                ret.getIndexingPathToRules().put(new ObjectJsonPath(key), rules);
            }
        }
        ret.setPrimaryKeyPath(getPath((String)obj.get("primary-key-path")));
        // Relations
        @SuppressWarnings("unchecked")
        Map<String, Object> relationPathToRules = 
                (Map<String, Object>)obj.get("relation-path-to-rules");
        if (relationPathToRules != null) {
            ret.setRelationPathToRules(new LinkedHashMap<>());
            for (String key : relationPathToRules.keySet()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> rulesObj = (Map<String, Object>)relationPathToRules.get(key);
                RelationRules rules = new RelationRules();
                rules.setTargetObjectType((String)rulesObj.get("target-object-type"));
                rules.setRelationType((String)rulesObj.get("relation-type"));
                ret.getRelationPathToRules().put(new ObjectJsonPath(key), rules);
            }
        }
        return ret;
    }
    
    private static ObjectJsonPath getPath(String path) throws ObjectParseException {
        return path == null ? null : new ObjectJsonPath(path);
    }
}
