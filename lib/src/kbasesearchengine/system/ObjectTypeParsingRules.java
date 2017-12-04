package kbasesearchengine.system;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.parse.ObjectParseException;
import kbasesearchengine.system.IndexingRules.Builder;
import kbasesearchengine.tools.Utils;
import us.kbase.common.service.UObject;

public class ObjectTypeParsingRules {
    
    //TODO TEST
    //TODO CODE make fields final and use builder instead of setters
    //TODO CODE not sure if throwing ObjectParseException makes sense here
    //TODO NNOW what if there are two parent types for a single type? need to error out?
    
    private String globalObjectType;
    private String uiTypeName;
    private StorageObjectType storageObjectType;
    private String innerSubType;
    private ObjectJsonPath pathToSubObjects;
    private List<IndexingRules> indexingRules;
    private ObjectJsonPath primaryKeyPath;
    
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
        try {
            ObjectTypeParsingRules ret = new ObjectTypeParsingRules();
            ret.setGlobalObjectType((String)obj.get("global-object-type"));
            ret.setUiTypeName((String)obj.get("ui-type-name"));
            final String storageCode = (String)obj.get("storage-type");
            final String type = (String)obj.get("storage-object-type");
            if (Utils.isNullOrEmpty(storageCode)) {
                throw new ObjectParseException(getMissingKeyParseMessage("storage-type"));
            }
            if (Utils.isNullOrEmpty(type)) {
                throw new ObjectParseException(getMissingKeyParseMessage("storage-object-type"));
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
                    final String path = (String) rulesObj.get("path");
                    final String keyName = (String) rulesObj.get("key-name");
                    final Builder builder;
                    if (Utils.isNullOrEmpty(path)) {
                        final String sourceKey = (String)rulesObj.get("source-key");
                        builder = IndexingRules.fromSourceKey(sourceKey, keyName);
                    } else {
                        //TODO CODE throw exception if sourceKey != null?
                        builder = IndexingRules.fromPath(new ObjectJsonPath(path));
                        if (!Utils.isNullOrEmpty(keyName)) {
                            builder.withKeyName(keyName);
                        }
                    }
                    if (getBool((Boolean) rulesObj.get("from-parent"))) {
                        //TODO NNOW throw exception if not a sub type
                        builder.withFromParent();
                    }
                    if (getBool(rulesObj.get("full-text"))) {
                        builder.withFullText();
                    }
                    final String keywordType = (String)rulesObj.get("keyword-type");
                    if (!Utils.isNullOrEmpty(keywordType)) {
                        //TODO CODE throw an error if fullText is true?
                        builder.withKeywordType(keywordType);
                    }
                    final String transform = (String) rulesObj.get("transform");
                    final String subObjectIDKey = (String) rulesObj.get("subobject-id-key");
                    final String targetObjectType = (String) rulesObj.get("target-object-type");
                    builder.withNullableUnknownTransform(
                            transform, targetObjectType, subObjectIDKey);
                    if (getBool(rulesObj.get("not-indexed"))) {
                        builder.withNotIndexed();
                    }
                    builder.withNullableDefaultValue(rulesObj.get("optional-default-value"));
                    builder.withNullableUIName((String) rulesObj.get("ui-name"));
                    if (getBool(rulesObj.get("ui-hidden"))) {
                        builder.withUIHidden();
                    }
                    builder.withNullableUILinkKey((String) rulesObj.get("ui-link-key"));
                    ret.getIndexingRules().add(builder.build());
                }
            }
            ret.setPrimaryKeyPath(getPath((String)obj.get("primary-key-path")));
            return ret;
        } catch (ObjectParseException | IllegalArgumentException | NullPointerException e) {
            throw new ObjectParseException(String.format("Error in source %s: %s",
                    sourceInfo, e.getMessage()), e);
        }
    }
    
    private static boolean getBool(final Object putativeBool) {
        //TODO CODE precheck cast exception
        return putativeBool != null && (Boolean) putativeBool; 
    }
    
    private static String getMissingKeyParseMessage(final String key) {
        return String.format("Missing key %s", key);
    }

    private static ObjectJsonPath getPath(String path) throws ObjectParseException {
        return path == null ? null : new ObjectJsonPath(path);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ObjectTypeParsingRules [globalObjectType=");
        builder.append(globalObjectType);
        builder.append(", uiTypeName=");
        builder.append(uiTypeName);
        builder.append(", storageObjectType=");
        builder.append(storageObjectType);
        builder.append(", innerSubType=");
        builder.append(innerSubType);
        builder.append(", pathToSubObjects=");
        builder.append(pathToSubObjects);
        builder.append(", indexingRules=");
        builder.append(indexingRules);
        builder.append(", primaryKeyPath=");
        builder.append(primaryKeyPath);
        builder.append("]");
        return builder.toString();
    }
}
