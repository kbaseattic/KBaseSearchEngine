package kbasesearchengine.system;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Optional;

import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.tools.Utils;

/**
 * A set of rules that determine how to extract and transform portions of a data object into
 * an indexable form.
 * 
 * The rules can apply to the parent object or to a subobject contained within the parent object.
 * There are usually many subobjects with identical fields stored in a list or map inside the
 * parent object.
 * 
 * @see ObjectTypeParsingRulesFileParser
 * @author gaprice@lbl.gov
 *
 */
public class ObjectTypeParsingRules {
    
    //TODO IDXRULE what if there are two parent types for a single type? need to error out?
    //TODO IDXRULE if subobject spec, there must be a path based indexing rule that matches the subobject id path. Make this automatic in some way?
    
    private final SearchObjectType globalObjectType;
    private final String uiTypeName;
    private final StorageObjectType storageObjectType;
    private final List<IndexingRules> indexingRules;
    private final Optional<String> subObjectType;
    private final Optional<ObjectJsonPath> subObjectPath;
    private final Optional<ObjectJsonPath> subObjectIDPath;
    
    
    private ObjectTypeParsingRules(
            final SearchObjectType globalObjectType,
            String uiTypeName,
            final StorageObjectType storageObjectType,
            final List<IndexingRules> indexingRules,
            final String subObjectType,
            final ObjectJsonPath subObjectPath,
            final ObjectJsonPath subObjectIDPath) {
        this.globalObjectType = globalObjectType;
        if (uiTypeName == null) {
            uiTypeName = globalObjectType.getType().substring(0, 1).toUpperCase() +
                    globalObjectType.getType().substring(1);
        }
        this.uiTypeName = uiTypeName;
        this.storageObjectType = storageObjectType;
        this.subObjectType = Optional.fromNullable(subObjectType);
        this.subObjectPath = Optional.fromNullable(subObjectPath);
        this.indexingRules = Collections.unmodifiableList(indexingRules);
        this.subObjectIDPath = Optional.fromNullable(subObjectIDPath);
    }

    /** Get the type of object this rule set applies to as known to the search system.
     * @return the search object type.
     */
    public SearchObjectType getGlobalObjectType() {
        return globalObjectType;
    }
    
    /** Get the name of the type to be displayed to the user interface.
     * @return the UI name.
     */
    public String getUiTypeName() {
        return uiTypeName;
    }
    
    /** Get the type of object this rule set applies to as known to the source of the data.
     * @return the data source object type.
     */
    public StorageObjectType getStorageObjectType() {
        return storageObjectType;
    }
    
    /** Get the set of indexing rules to apply to the object or subobjects.
     * @return the indexingn rules.
     */
    public List<IndexingRules> getIndexingRules() {
        return indexingRules;
    }

    /** Get the type of the subobject as known to the search system, or absent if this rule set
     * applies to the parent object.
     * @return the subobject type.
     */
    public Optional<String> getSubObjectType() {
        return subObjectType;
    }
    
    /** Get the path to the subobjects in the parent object, or absent if this rule set applies
     * to the parent object.
     * @return the path to the subobjects.
     */
    public Optional<ObjectJsonPath> getSubObjectPath() {
        return subObjectPath;
    }
    
    /** Get the path to the ids of the subobjects, or absent if this rule set applies to the
     * parent objects. The path is relative to the subobject root.
     * @return the path to the subobject ids.
     */
    public Optional<ObjectJsonPath> getSubObjectIDPath() {
        return subObjectIDPath;
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
                + ((subObjectType == null) ? 0 : subObjectType.hashCode());
        result = prime * result + ((subObjectPath == null) ? 0
                : subObjectPath.hashCode());
        result = prime * result
                + ((subObjectIDPath == null) ? 0 : subObjectIDPath.hashCode());
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
        if (subObjectType == null) {
            if (other.subObjectType != null) {
                return false;
            }
        } else if (!subObjectType.equals(other.subObjectType)) {
            return false;
        }
        if (subObjectPath == null) {
            if (other.subObjectPath != null) {
                return false;
            }
        } else if (!subObjectPath.equals(other.subObjectPath)) {
            return false;
        }
        if (subObjectIDPath == null) {
            if (other.subObjectIDPath != null) {
                return false;
            }
        } else if (!subObjectIDPath.equals(other.subObjectIDPath)) {
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
    
    /** Get a builder for a new {@link ObjectTypeParsingRules} instance.
     * @param globalObjectType the local, search system type to which the rules apply.
     * @param storageType the data source type to which the rules apply.
     * @return a new builder.
     */
    public static Builder getBuilder(
            final SearchObjectType globalObjectType,
            final StorageObjectType storageType) {
        return new Builder(globalObjectType, storageType);
    }
    
    /** A builder for an {@link ObjectTypeParsingRules}.
     * @author gaprice@lbl.gov
     *
     */
    public static class Builder {
        
        private final SearchObjectType globalObjectType;
        private String uiTypeName; 
        private final StorageObjectType storageObjectType;
        private final List<IndexingRules> indexingRules = new LinkedList<>();
        private String subObjectType = null;
        private ObjectJsonPath subObjectPath = null;
        private ObjectJsonPath subObjectIDPath = null;
        
        private Builder(final SearchObjectType globalObjectType, final StorageObjectType storageType) {
            Utils.nonNull(globalObjectType, "globalObjectType");
            Utils.nonNull(storageType, "storageType");
            this.globalObjectType = globalObjectType;
            this.storageObjectType = storageType;
        }
        
        /** Set the name of the type of data associated with this rule set as it should be
         * displayed in a user interface. Nulls and whitespace are ignored. By default, the name
         * is set to be the global object type
         * (see {@link ObjectTypeParsingRules#getBuilder(String, StorageObjectType)}) with the
         * first character capitalized.
         * @param uiTypeName the UI type name.
         * @return this builder.
         */
        public Builder withNullableUITypeName(final String uiTypeName) {
            if (!Utils.isNullOrEmpty(uiTypeName)) {
                this.uiTypeName = uiTypeName;
            }
            return this;
        }
        
        /** Add an indexing rule to this builder.
         * @param rules the indexing rule.
         * @return this builder.
         */
        public Builder withIndexingRule(final IndexingRules rules) {
            Utils.nonNull(rules, "rules");
            if (rules.isFromParent() && subObjectType == null) {
                throw new IllegalArgumentException("Cannot supply an indexing rule that " +
                        "extracts data from a parent to a rule set that applies to the parent");
            }
            indexingRules.add(rules);
            return this;
        }
        
        /** Convert this rule set to a set that applies to a subobject of the parent object as
         * opposed to the parent object itself.
         * Call this method before adding any indexing rules
         * ({@link #withIndexingRule(IndexingRules)})
         * where {@link IndexingRules#isFromParent()} is true.
         * @param subObjectType the local, search type of the subobject.
         * @param subObjectPath the path to the subobjects inside the parent object.
         * @param subObjectIDPath the path from the root of the subobject to the id to be used
         * for the subobject.
         * @return this builder.
         */
        public Builder toSubObjectRule(
                final String subObjectType,
                final ObjectJsonPath subObjectPath,
                final ObjectJsonPath subObjectIDPath) {
            Utils.nonNull(subObjectPath, "subObjectPath");
            Utils.nonNull(subObjectIDPath, "subObjectIDPath");
            Utils.notNullOrEmpty(subObjectType, "subObjectType cannot be null or whitespace");
            this.subObjectPath = subObjectPath;
            this.subObjectIDPath = subObjectIDPath;
            this.subObjectType = subObjectType;
            return this;
        }
        
        /** Returns the number of indexing rules added to the builder so far.
         * @return the indexing rules.
         */
        public int numberOfIndexingRules() {
            return indexingRules.size();
        }
        
        /** Build the parsing rule set.
         * @return the rule set.
         */
        public ObjectTypeParsingRules build() {
            return new ObjectTypeParsingRules(globalObjectType, uiTypeName, storageObjectType,
                    indexingRules, subObjectType, subObjectPath, subObjectIDPath);
        }
    }
}
