package kbaserelationengine.system;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import kbaserelationengine.common.ObjectJsonPath;
import kbaserelationengine.parse.ObjectParseException;
import kbaserelationengine.tools.Utils;
import us.kbase.common.service.UObject;

public class ObjectTypeParsingRules {
    
    //TODO TEST
    //TODO CODE make fields final and use builder instead of setters
    
    private String globalObjectType;
    private String uiTypeName;
    private StorageObjectType storageObjectType;
    private String innerSubType;
    private ObjectJsonPath pathToSubObjects;
    private List<IndexingRules> indexingRules;
    private ObjectJsonPath primaryKeyPath;
    private List<RelationRules> relationRules;
    
    public String getGlobalObjectType() {
        return globalObjectType;
    }
    
    private void setGlobalObjectType(String globalObjectType) {
        this.globalObjectType = globalObjectType;
    }
    
    public String getUiTypeName() {
        return uiTypeName;
    }
    
    private void setUiTypeName(String uiTypeName) {
        this.uiTypeName = uiTypeName;
    }
    
    // cannot be null
    public StorageObjectType getStorageObjectType() {
        return storageObjectType;
    }
    
    private void setStorageObjectType(StorageObjectType storageObjectType) {
        this.storageObjectType = storageObjectType;
    }
    
    public String getInnerSubType() {
        return innerSubType;
    }
    
    private void setInnerSubType(String innerSubType) {
        this.innerSubType = innerSubType;
    }
    
    public ObjectJsonPath getPathToSubObjects() {
        return pathToSubObjects;
    }
    
    private void setPathToSubObjects(ObjectJsonPath pathToSubObjects) {
        this.pathToSubObjects = pathToSubObjects;
    }

    public List<IndexingRules> getIndexingRules() {
        return indexingRules;
    }
    
    private void setIndexingRules(List<IndexingRules> indexingRules) {
        this.indexingRules = indexingRules;
    }
    
    public ObjectJsonPath getPrimaryKeyPath() {
        return primaryKeyPath;
    }
    
    private void setPrimaryKeyPath(ObjectJsonPath primaryKeyPath) {
        this.primaryKeyPath = primaryKeyPath;
    }
    
    public List<RelationRules> getRelationRules() {
        return relationRules;
    }
    
    private void setRelationRules(
            List<RelationRules> foreignKeyLookupRules) {
        this.relationRules = foreignKeyLookupRules;
    }

    public static ObjectTypeParsingRules fromFile(final File file) 
            throws ObjectParseException, IOException {
        try (final InputStream is = new FileInputStream(file)) {
            return fromStream(is, file.toString());
        }
    }

    public static ObjectTypeParsingRules fromJson(String json) throws ObjectParseException {
        @SuppressWarnings("unchecked")
        Map<String, Object> obj = UObject.transformStringToObject(json, Map.class);
        return fromObject(obj, "json");
    }

    public static ObjectTypeParsingRules fromStream(InputStream is, String sourceInfo) 
            throws IOException, ObjectParseException {
        @SuppressWarnings("unchecked")
        Map<String, Object> obj = UObject.getMapper().readValue(is, Map.class);
        return fromObject(obj, sourceInfo);
    }

    public static ObjectTypeParsingRules fromObject(
            final Map<String, Object> obj,
            final String sourceInfo) 
            throws ObjectParseException {
        ObjectTypeParsingRules ret = new ObjectTypeParsingRules();
        ret.setGlobalObjectType((String)obj.get("global-object-type"));
        ret.setUiTypeName((String)obj.get("ui-type-name"));
        final String storageCode = (String)obj.get("storage-type");
        final String type = (String)obj.get("storage-object-type");
        if (Utils.isNullOrEmpty(storageCode)) {
            throw new ObjectParseException(getMissingKeyParseMessage(
                    "storage-type", sourceInfo));
        }
        if (Utils.isNullOrEmpty(type)) {
            throw new ObjectParseException(getMissingKeyParseMessage(
                    "storage-object-type", sourceInfo));
        }
        ret.setStorageObjectType(new StorageObjectType(storageCode, type));
        ret.setInnerSubType((String)obj.get("inner-sub-type"));
        ret.setPathToSubObjects(getPath((String)obj.get("path-to-sub-objects")));
        // Indexing
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> indexingRules = 
                (List<Map<String, Object>>)obj.get("indexing-rules");
        if (indexingRules != null) {
            ret.setIndexingRules(new ArrayList<>());
            for (Map<String, Object> rulesObj : indexingRules) {
                IndexingRules rules = new IndexingRules();
                String path = (String)rulesObj.get("path");
                if (path != null) {
                    rules.setPath(new ObjectJsonPath(path));
                }
                Boolean fullText = (Boolean)rulesObj.get("full-text");
                if (fullText != null) {
                    rules.setFullText(fullText);
                }
                rules.setKeywordType((String)rulesObj.get("keyword-type"));
                rules.setKeyName((String)rulesObj.get("key-name"));
                rules.setTransform((String)rulesObj.get("transform"));
                Boolean fromParent = (Boolean)rulesObj.get("from-parent");
                if (fromParent != null) {
                    rules.setFromParent(fromParent);
                }
                Boolean derivedKey = (Boolean)rulesObj.get("derived-key");
                if (derivedKey != null) {
                    rules.setDerivedKey(derivedKey);
                }
                Boolean notIndexed = (Boolean)rulesObj.get("not-indexed");
                if (notIndexed != null) {
                    rules.setNotIndexed(notIndexed);
                }
                rules.setSourceKey((String)rulesObj.get("source-key"));
                rules.setTargetObjectType((String)rulesObj.get("source-key"));
                rules.setSubobjectIdKey((String)rulesObj.get("subobject-id-key"));
                rules.setConstantValue(rulesObj.get("constant-value"));
                rules.setOptionalDefaultValue(rulesObj.get("optional-default-value"));
                rules.setTargetObjectType((String)rulesObj.get("target-object-type"));
                rules.setUiName((String)rulesObj.get("ui-name"));
                Boolean uiHidden = (Boolean)rulesObj.get("ui-hidden");
                if (uiHidden != null) {
                    rules.setUiHidden(uiHidden);
                }
                rules.setUiLinkKey((String)rulesObj.get("ui-link-key"));
                ret.getIndexingRules().add(rules);
            }
        }
        ret.setPrimaryKeyPath(getPath((String)obj.get("primary-key-path")));
        // Relations
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> relationRules = 
                (List<Map<String, Object>>)obj.get("relation-rules");
        if (relationRules != null) {
            ret.setRelationRules(new ArrayList<>());
            for (Map<String, Object> rulesObj : relationRules) {
                RelationRules rules = new RelationRules();
                String pathText = (String)rulesObj.get("path");
                if (pathText != null) {
                    rules.setPath(new ObjectJsonPath(pathText));
                }
                rules.setTargetObjectType((String)rulesObj.get("target-object-type"));
                rules.setRelationType((String)rulesObj.get("relation-type"));
                ret.getRelationRules().add(rules);
            }
        }
        return ret;
    }
    
    private static String getMissingKeyParseMessage(final String key, final String sourceInfo) {
        return String.format("Missing key %s.%s", key, Utils.isNullOrEmpty(sourceInfo) ?
                "" : "Source : " + sourceInfo);
    }

    private static ObjectJsonPath getPath(String path) throws ObjectParseException {
        return path == null ? null : new ObjectJsonPath(path);
    }
}
