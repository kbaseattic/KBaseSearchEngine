package kbasesearchengine.system;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;

import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.parse.ObjectParseException;
import kbasesearchengine.tools.Utils;
import us.kbase.common.service.UObject;

public class ObjectTypeParsingRules {
    
    //TODO JAVADOC
    //TODO TEST
    //TODO CODE not sure if throwing ObjectParseException makes sense here
    //TODO NNOW what if there are two parent types for a single type? need to error out?
    
    private final String globalObjectType;
    private final String uiTypeName;
    private final StorageObjectType storageObjectType;
    private final Optional<String> innerSubType;
    private final Optional<ObjectJsonPath> pathToSubObjects;
    private final List<IndexingRules> indexingRules;
    private final Optional<ObjectJsonPath> primaryKeyPath;
    
    
    private ObjectTypeParsingRules(
            final String globalObjectType,
            String uiTypeName,
            final StorageObjectType storageObjectType,
            final String innerSubType,
            final ObjectJsonPath pathToSubObjects,
            final List<IndexingRules> indexingRules,
            final ObjectJsonPath primaryKeyPath) {
        this.globalObjectType = globalObjectType;
        if (uiTypeName == null) {
            uiTypeName = globalObjectType.substring(0, 1).toUpperCase() +
                    globalObjectType.substring(1);
        }
        this.uiTypeName = uiTypeName;
        this.storageObjectType = storageObjectType;
        this.innerSubType = Optional.fromNullable(innerSubType);
        this.pathToSubObjects = Optional.fromNullable(pathToSubObjects);
        this.indexingRules = Collections.unmodifiableList(indexingRules);
        this.primaryKeyPath = Optional.fromNullable(primaryKeyPath);
    }

    public String getGlobalObjectType() {
        return globalObjectType;
    }
    
    public String getUiTypeName() {
        return uiTypeName;
    }
    
    public StorageObjectType getStorageObjectType() {
        return storageObjectType;
    }
    
    public Optional<String> getInnerSubType() {
        return innerSubType;
    }
    
    public Optional<ObjectJsonPath> getPathToSubObjects() {
        return pathToSubObjects;
    }
    
    public List<IndexingRules> getIndexingRules() {
        return indexingRules;
    }
    
    public Optional<ObjectJsonPath> getPrimaryKeyPath() {
        return primaryKeyPath;
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
            final String storageCode = (String)obj.get("storage-type");
            final String type = (String)obj.get("storage-object-type");
            if (Utils.isNullOrEmpty(storageCode)) {
                throw new ObjectParseException(getMissingKeyParseMessage("storage-type"));
            }
            if (Utils.isNullOrEmpty(type)) {
                throw new ObjectParseException(getMissingKeyParseMessage("storage-object-type"));
            }
            final Builder builder = ObjectTypeParsingRules.getBuilder(
                    (String) obj.get("global-object-type"), //TODO CODE better error if missing
                    new StorageObjectType(storageCode, type))
                    .withNullableUITypeName((String)obj.get("ui-type-name"));
            final String subType = (String)obj.get("inner-sub-type");
            if (!Utils.isNullOrEmpty(subType)) {
                builder.toSubObjectRule(
                        //TODO CODE add checks to ensure these exist
                        getPath((String)obj.get("path-to-sub-objects")),
                        getPath((String)obj.get("primary-key-path")),
                        subType);
            } // throw exception if the other subobj values exist?
            // Indexing
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> indexingRules =
                    (List<Map<String, Object>>)obj.get("indexing-rules");
            if (indexingRules != null) {
                for (Map<String, Object> rulesObj : indexingRules) {
                    builder.withIndexingRule(buildRule(rulesObj));
                }
            }
            return builder.build();
        } catch (ObjectParseException | IllegalArgumentException | NullPointerException e) {
            throw new ObjectParseException(String.format("Error in source %s: %s",
                    sourceInfo, e.getMessage()), e);
        }
    }

    private static IndexingRules buildRule(
            final Map<String, Object> rulesObj)
            throws ObjectParseException {
        final String path = (String) rulesObj.get("path");
        final String keyName = (String) rulesObj.get("key-name");
        final IndexingRules.Builder irBuilder;
        if (Utils.isNullOrEmpty(path)) {
            final String sourceKey = (String)rulesObj.get("source-key");
            irBuilder = IndexingRules.fromSourceKey(sourceKey, keyName);
        } else {
            //TODO CODE throw exception if sourceKey != null?
            irBuilder = IndexingRules.fromPath(new ObjectJsonPath(path));
            if (!Utils.isNullOrEmpty(keyName)) {
                irBuilder.withKeyName(keyName);
            }
        }
        if (getBool((Boolean) rulesObj.get("from-parent"))) {
            //TODO NNOW throw exception if not a sub type
            irBuilder.withFromParent();
        }
        if (getBool(rulesObj.get("full-text"))) {
            irBuilder.withFullText();
        }
        final String keywordType = (String)rulesObj.get("keyword-type");
        if (!Utils.isNullOrEmpty(keywordType)) {
            //TODO CODE throw an error if fullText is true?
            irBuilder.withKeywordType(keywordType);
        }
        final String transform = (String) rulesObj.get("transform");
        if (!Utils.isNullOrEmpty(transform)) {
            final String subObjectIDKey = (String) rulesObj.get("subobject-id-key");
            final String targetObjectType =
                    (String) rulesObj.get("target-object-type");
            final String[] tranSplt = transform.split("\\.", 2);
            final String transProp = tranSplt.length == 1 ? null : tranSplt[1];
            irBuilder.withTransform(Transform.unknown(
                    tranSplt[0], transProp, targetObjectType, subObjectIDKey));
        }
        if (getBool(rulesObj.get("not-indexed"))) {
            irBuilder.withNotIndexed();
        }
        irBuilder.withNullableDefaultValue(rulesObj.get("optional-default-value"));
        irBuilder.withNullableUIName((String) rulesObj.get("ui-name"));
        if (getBool(rulesObj.get("ui-hidden"))) {
            irBuilder.withUIHidden();
        }
        irBuilder.withNullableUILinkKey((String) rulesObj.get("ui-link-key"));
        return irBuilder.build();
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((globalObjectType == null) ? 0
                : globalObjectType.hashCode());
        result = prime * result
                + ((indexingRules == null) ? 0 : indexingRules.hashCode());
        result = prime * result
                + ((innerSubType == null) ? 0 : innerSubType.hashCode());
        result = prime * result + ((pathToSubObjects == null) ? 0
                : pathToSubObjects.hashCode());
        result = prime * result
                + ((primaryKeyPath == null) ? 0 : primaryKeyPath.hashCode());
        result = prime * result + ((storageObjectType == null) ? 0
                : storageObjectType.hashCode());
        result = prime * result
                + ((uiTypeName == null) ? 0 : uiTypeName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ObjectTypeParsingRules other = (ObjectTypeParsingRules) obj;
        if (globalObjectType == null) {
            if (other.globalObjectType != null) {
                return false;
            }
        } else if (!globalObjectType.equals(other.globalObjectType)) {
            return false;
        }
        if (indexingRules == null) {
            if (other.indexingRules != null) {
                return false;
            }
        } else if (!indexingRules.equals(other.indexingRules)) {
            return false;
        }
        if (innerSubType == null) {
            if (other.innerSubType != null) {
                return false;
            }
        } else if (!innerSubType.equals(other.innerSubType)) {
            return false;
        }
        if (pathToSubObjects == null) {
            if (other.pathToSubObjects != null) {
                return false;
            }
        } else if (!pathToSubObjects.equals(other.pathToSubObjects)) {
            return false;
        }
        if (primaryKeyPath == null) {
            if (other.primaryKeyPath != null) {
                return false;
            }
        } else if (!primaryKeyPath.equals(other.primaryKeyPath)) {
            return false;
        }
        if (storageObjectType == null) {
            if (other.storageObjectType != null) {
                return false;
            }
        } else if (!storageObjectType.equals(other.storageObjectType)) {
            return false;
        }
        if (uiTypeName == null) {
            if (other.uiTypeName != null) {
                return false;
            }
        } else if (!uiTypeName.equals(other.uiTypeName)) {
            return false;
        }
        return true;
    }
    
    public static Builder getBuilder(
            final String globalObjectType,
            final StorageObjectType storageType) {
        return new Builder(globalObjectType, storageType);
    }
    
    public static class Builder {
        
        private final String globalObjectType;
        private String uiTypeName; 
        private final StorageObjectType storageObjectType;
        private String innerSubType = null;
        private ObjectJsonPath pathToSubObjects = null;
        private final List<IndexingRules> indexingRules = new LinkedList<>();
        private ObjectJsonPath primaryKeyPath = null;
        
        public Builder(final String globalObjectType, final StorageObjectType storageType) {
            Utils.notNullOrEmpty(globalObjectType,
                    "globalObjectType cannot be null or whitespace");
            Utils.nonNull(storageType, "storageType");
            this.globalObjectType = globalObjectType;
            this.storageObjectType = storageType;
        }
        
        public Builder withNullableUITypeName(final String uiTypeName) {
            if (!Utils.isNullOrEmpty(uiTypeName)) {
                this.uiTypeName = uiTypeName;
            }
            return this;
        }
        
        public Builder withIndexingRule(final IndexingRules rules) {
            Utils.nonNull(rules, "rules");
            indexingRules.add(rules);
            return this;
        }
        
        public Builder toSubObjectRule(
                final ObjectJsonPath pathToSubObjects,
                final ObjectJsonPath primaryKeyPath,
                final String subObjectType) {
            Utils.nonNull(pathToSubObjects, "pathToSubObjects");
            Utils.nonNull(primaryKeyPath, "primaryKeyPath");
            Utils.notNullOrEmpty(subObjectType, "subObjectType cannot be null or whitespace");
            this.pathToSubObjects = pathToSubObjects;
            this.primaryKeyPath = primaryKeyPath;
            this.innerSubType = subObjectType;
            return this;
        }
        
        public int numberOfIndexingRules() {
            return indexingRules.size();
        }
        
        public ObjectTypeParsingRules build() {
            if (indexingRules.isEmpty()) {
                throw new IllegalStateException("Must supply at least one indexing rule");
            }
            
            return new ObjectTypeParsingRules(globalObjectType, uiTypeName, storageObjectType,
                    innerSubType, pathToSubObjects, indexingRules, primaryKeyPath);
        }
    }
}
