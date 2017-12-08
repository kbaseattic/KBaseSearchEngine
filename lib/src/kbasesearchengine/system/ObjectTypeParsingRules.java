package kbasesearchengine.system;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Optional;

import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.tools.Utils;

/**
 * 
 * @see ObjectTypeParsingRulesUtils
 * @author gaprice@lbl.gov
 *
 */
public class ObjectTypeParsingRules {
    
    //TODO JAVADOC
    //TODO TEST
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
