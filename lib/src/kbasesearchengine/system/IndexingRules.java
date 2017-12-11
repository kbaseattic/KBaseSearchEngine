package kbasesearchengine.system;

import com.google.common.base.Optional;

import kbasesearchengine.common.ObjectJsonPath;
import kbasesearchengine.tools.Utils;

/**
 * This class defines the rules for parsing source data, collecting portions
 * of it, and preparing it for indexing. A single instance of this class would
 * declare the indexing rules for a single keyword within a source object or
 * sub-object. A source object or sub-object would typically require many
 * indexing rules for the set of keywords that would need to be indexed for the
 * source (sub-)object.
 *
 * Each indexing rule forms a key-value pair based on some
 * content of the object (or sub-object) that it is to be indexed by the rules.
 * An indexing rule may also be based on a key-value pair formed by another
 * indexing rule.
 *
 */
public class IndexingRules {
    
    /**
     * Path to an element of the source object from which to form the keyword.
     * The path may contain "*" or "[*]" to collect an array of values for the
     * keyword. {size} is a special path item that gets the size of the array or
     * map that the path points to.
     *
     * Example: "ontology_terms/SSO/ * /id" extracts all id values from the
     * sub-elements of SSO
     *
     * Example: "features/{size}" extracts the feature count of the features
     * element.
     *
     */
    private final Optional<ObjectJsonPath> path;
    /**
     * fullText=true implies the use of the "text" type in ElasticSearch,
     * which stands for full text search (search on individual tokens) on the
     * extracted data. The extracted data in this case is assumed to be unstructured.
     * Example: search for "New" "York" in the data "New York".
     *
     * fullText=false implies the use of the "keyword" type in ElasticSearch,
     * which stands for the search of whole values on the extracted data.
     * The extracted data in this case is assumed to be structured.
     * Example: search for "New York" in the data "New York".
     *
     */
    private final boolean fullText;
    /**
     * The "keyword" type to use in ElasticSearch. This value must be specified
     * if fullText is set to false. Example: "integer", "string" etc.
     *
     * Defaults to "keyword" type in ElasticSearch if not defined and
     * fullText=false.
     *
     */
    private final Optional<String> keywordType;
    /**
     * Key name for keyword. If it is not specified, then first item between
     * slashes in "path" is used.
     *
     */
    private final String keyName;
    /** An optional transformation applied to a value.
     *
     */
    private final Optional<Transform> transform;
    
    /**
     * An optional attribute that indicates that the value is extracted from the
     * parent object rather that from the sub-object.
     *
     */
    private final boolean fromParent;
    /**
     * notIndexed=true indicates that value should be included into extracted part
     * of object data but must not be present as indexed keyword.
     *
     */
    private final boolean notIndexed;
    /**
     * An optional flag indicating that this rule defines a keyword formed by
     * another (source) keyword which is set in "source-key".
     *
     */
    private final Optional<String> sourceKey;

    /**
     * An optional value which is used for the keyword in case the resulting
     * array of values extracted from the document [sub-]object is empty.
     */
    private final Optional<Object> defaultValue;
    /**
     * Name of keyword displayed by UI.
     */
    private final String uiName;
    /**
     * An optional attribute that indicates that a particular keyword is not
     * supposed to be visible via UI though it could be used in API search queries.
     */
    private final boolean uiHidden;
    // TODO IDXRULE this should point to another indexing rule that is a guid.
    /**
     * An optional pointer to a paired keyword coupled with given one providing
     * GUID for making clickable URL for value provided.
     * Example: one keyword called "Genome name" may have "ui-link-key" pointer
     * to another keyword called "Genome GUID" so that Search UI will use
     * values of these two keywords in order to produce clickable link
     * showing genome name (coming from one keyword) and redirecting you to
     * landing page of given genome based on GUID (coming from another keyword).
     * The value of the indexing rule containing a uiLinkKey is expected to be the name
     * of the link, while the field provided in the uiLinkKey is expected to be a reference
     * to an object in a data store.
     */
    private final Optional<String> uiLinkKey;
    
    private IndexingRules(
            final ObjectJsonPath path,
            final boolean fullText,
            final String keywordType,
            final String keyName,
            final Transform transform,
            final boolean fromParent,
            final boolean notIndexed,
            final String sourceKey,
            final Object defaultValue,
            String uiName,
            final boolean uiHidden,
            final String uiLinkKey) {
        this.path = Optional.fromNullable(path);
        this.fullText = fullText;
        this.keywordType = Optional.fromNullable(keywordType);
        this.keyName = keyName;
        this.transform = Optional.fromNullable(transform);
        this.fromParent = fromParent;
        this.notIndexed = notIndexed;
        this.sourceKey = Optional.fromNullable(sourceKey);
        this.defaultValue = Optional.fromNullable(defaultValue);
        if (uiName == null) {
            uiName = keyName.substring(0, 1).toUpperCase() + keyName.substring(1);
        }
        this.uiName = uiName;
        this.uiHidden = uiHidden;
        this.uiLinkKey = Optional.fromNullable(uiLinkKey);
    }

    /** Returns the path into an object to a value that is the target of this indexing rule.
     * If a path is provided, {@link #getSourceKey()} will return absent.
     * @return the path.
     */
    public Optional<ObjectJsonPath> getPath() {
        return path;
    }
    
    /** Returns true if the value associated with this indexing rule should be indexed as full
     * text rather than a keyword. If true, {@link #getKeywordType()} will return absent.
     * @return true if this is a full text indexing rule.
     */
    public boolean isFullText() {
        return fullText;
    }
    
    /** Get the keyword type (e.g. integer, boolean, keyword, etc.) that specifies how the value
     * associated with this indexing rule should be indexed. If non-absent, {@link #isFullText()}
     * will return false.
     * @return the keyword type.
     */
    public Optional<String> getKeywordType() {
        return keywordType;
    }
    
    /** Get the name of the key under which the result of this indexing rule will be indexed.
     * @return the name of the key.
     */
    public String getKeyName() {
        return keyName;
    }
    
    /** Get the transform associated with this indexing rule, if any.
     * @return the transform.
     */
    public Optional<Transform> getTransform() {
        return transform;
    }
    
    /** Returns true if the operations specified with this indexing rule should be applied to
     * the parent object of a subobject rather than the subobject itself.
     * @return true if the operations should be applied to the parent object.
     */
    public boolean isFromParent() {
        return fromParent;
    }
    
    /** Returns true if the value associated with this indexing rule is derived from another
     * indexing rule. In this case, {@link #getSourceKey()} will return the name of the key of the
     * value of interest in the target indexing rule, and {@link #getPath()} will return absent.
     * @return true if this is indexing rule operates on a value associated with another indexing
     * rule.
     */
    public boolean isDerivedKey() {
        return sourceKey.isPresent();
    }
    
    /** Returns true if the value associated with this key should not be indexed.
     * @return true if the value this indexing rule produces should not be indexed.
     */
    public boolean isNotIndexed() {
        return notIndexed;
    }
    
    /** Get the key name of the indexing rule associated with the value upon which this value
     * will operate. If this method returns a non-absent value, {@link #isDerivedKey()} will
     * return true.
     * @return the key name for the data source for this indexing rule.
     */
    public Optional<String> getSourceKey() {
        return sourceKey;
    }
    
    /** Get the default value for this indexing rule if no value is available, if any.
     * @return the default value or absent if no default value was set.
     */
    public Optional<Object> getDefaultValue() {
        return defaultValue;
    }
    
    /** Get the name for this indexing rule to be show in a UI context.
     * @return the UI name.
     */
    public String getUiName() {
        return uiName;
    }
    
    /** Returns true if the value for this indexing rule should not be displayed in a UI
     * context.
     * @return true if the value should not be displayed.
     */
    public boolean isUiHidden() {
        return uiHidden;
    }
    
    //TODO IDXRULE does this have to point to an indexing rule with a GUID, e.g. the indexing rule has a GUID transform?
    /** Get, if present, the key name of an indexing rule that contains a GUID addressing a
     * data object to which the value associated with this indexing rule should be linked in a UI
     * context. For example, the value associated with this indexing rule might contain a product
     * name, while the indexing rule specified by the key name returned by this method is
     * associated with a GUID with the data store address of a data object. In the UI, the a link
     * would be created with the target being the data store address and the name of the link
     * the value associated with this indexing rule.
     * @return the key name of an indexing rule associated with a GUID.
     */
    public Optional<String> getUiLinkKey() {
        return uiLinkKey;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((defaultValue == null) ? 0 : defaultValue.hashCode());
        result = prime * result + (fromParent ? 1231 : 1237);
        result = prime * result + (fullText ? 1231 : 1237);
        result = prime * result + ((keyName == null) ? 0 : keyName.hashCode());
        result = prime * result
                + ((keywordType == null) ? 0 : keywordType.hashCode());
        result = prime * result + (notIndexed ? 1231 : 1237);
        result = prime * result + ((path == null) ? 0 : path.hashCode());
        result = prime * result
                + ((sourceKey == null) ? 0 : sourceKey.hashCode());
        result = prime * result
                + ((transform == null) ? 0 : transform.hashCode());
        result = prime * result + (uiHidden ? 1231 : 1237);
        result = prime * result
                + ((uiLinkKey == null) ? 0 : uiLinkKey.hashCode());
        result = prime * result + ((uiName == null) ? 0 : uiName.hashCode());
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
        IndexingRules other = (IndexingRules) obj;
        if (defaultValue == null) {
            if (other.defaultValue != null) {
                return false;
            }
        } else if (!defaultValue.equals(other.defaultValue)) {
            return false;
        }
        if (fromParent != other.fromParent) {
            return false;
        }
        if (fullText != other.fullText) {
            return false;
        }
        if (keyName == null) {
            if (other.keyName != null) {
                return false;
            }
        } else if (!keyName.equals(other.keyName)) {
            return false;
        }
        if (keywordType == null) {
            if (other.keywordType != null) {
                return false;
            }
        } else if (!keywordType.equals(other.keywordType)) {
            return false;
        }
        if (notIndexed != other.notIndexed) {
            return false;
        }
        if (path == null) {
            if (other.path != null) {
                return false;
            }
        } else if (!path.equals(other.path)) {
            return false;
        }
        if (sourceKey == null) {
            if (other.sourceKey != null) {
                return false;
            }
        } else if (!sourceKey.equals(other.sourceKey)) {
            return false;
        }
        if (transform == null) {
            if (other.transform != null) {
                return false;
            }
        } else if (!transform.equals(other.transform)) {
            return false;
        }
        if (uiHidden != other.uiHidden) {
            return false;
        }
        if (uiLinkKey == null) {
            if (other.uiLinkKey != null) {
                return false;
            }
        } else if (!uiLinkKey.equals(other.uiLinkKey)) {
            return false;
        }
        if (uiName == null) {
            if (other.uiName != null) {
                return false;
            }
        } else if (!uiName.equals(other.uiName)) {
            return false;
        }
        return true;
    }

    /** Get a builder for an {@link IndexingRules} instance based on a JSON path into an object.
     * The key name (see {@link #getKeyName()} and {@link Builder#withKeyName(String)} is set
     * as the first portion of the path, but can be changed with
     * {@link Builder#withKeyName(String)}.
     * @param path the path to the value of interest in the object for the new
     * {@link IndexingRules}.
     * @return a new builder.
     */
    public static Builder fromPath(final ObjectJsonPath path) {
        return new Builder(path);
    }
    
    //TODO IDXRULE do source key rules have to occur later in the ordering than their target key?
    /** Get a builder for an {@link IndexingRules} instance based on another {@link IndexingRules}
     * specified by a the sourceKey.
     * @param sourceKey the keyName of the {@link IndexingRules} of the value of interest.
     * @param keyName the name of the key (see {@link #getKeyName()} for this indexing rule.
     * @return a new builder.
     */
    public static Builder fromSourceKey(final String sourceKey, final String keyName) {
        return new Builder(sourceKey, keyName);
    }
    
    /** A builder for {@link IndexingRules}.
     * @author gaprice@lbl.gov
     *
     */
    public static class Builder {
        
        private String uiName = null;
        private final ObjectJsonPath path;
        private final String sourceKey;
        private String keyName;
        private boolean fullText = false;
        private String keywordType = "keyword";
        private Transform transform = null;
        private boolean fromParent = false;
        private boolean notIndexed = false;
        private Object defaultValue = null;
        private boolean uiHidden = false;
        private String uiLinkKey = null;
        
        private Builder(final ObjectJsonPath path) {
            Utils.nonNull(path, "path");
            this.path = path;
            sourceKey = null;
            keyName = path.getPathItems()[0];
        }
        
        private Builder(final String sourceKey, final String keyName) {
            Utils.notNullOrEmpty(sourceKey, "sourceKey cannot be null or whitespace");
            Utils.notNullOrEmpty(keyName, "keyName cannot be null or whitespace");
            this.path = null;
            this.sourceKey = sourceKey;
            this.keyName = keyName;
        }
        
        /** Change the key name for this indexing rule.
         * @param keyName the new key name.
         * @return this builder.
         */
        public Builder withKeyName(final String keyName) {
            Utils.notNullOrEmpty(keyName, "keyName cannot be null or whitespace");
            this.keyName = keyName;
            return this;
        }
        
        /** Set this indexing rule to a full text rule. This means that
         * {@link IndexingRules#getKeywordType()} will return absent.
         * @return this builder.
         */
        public Builder withFullText() {
            this.keywordType = null;
            this.fullText = true;
            return this;
        }
        
        //TODO CODE this might be a candidate for an enum
        /** Set the type of the key word to be indexed. Default is "keyword." Absent if full text
         * is set.
         * @param keywordType the keyword type.
         * @return this builder.
         */
        public Builder withKeywordType(final String keywordType) {
            Utils.notNullOrEmpty(keywordType, "keywordType cannot be null or whitespace");
            this.keywordType = keywordType;
            this.fullText = false;
            return this;
        }
        
        /** Add a transform to this indexing rule. Note that GUID transforms with subobject ID keys
         * are only allowed with source key based indexing rules (i.e. the builder creation
         * method was {@link IndexingRules#fromSourceKey(String, String)}).
         * @param transform the transform to add.
         * @return this builder.
         */
        public Builder withTransform(final Transform transform) {
            Utils.nonNull(transform, "transform");
            // not clear why this is required, but this constraint was in original code
            if (transform.getSubobjectIdKey().isPresent() && path != null) {
                throw new IllegalArgumentException(
                        "A transform with a subobject ID key is not compatible with a path. " +
                        "Path is: " + path);
            }
            this.transform = transform;
            return this;
        }
        
        /** Specify that the value produced by this indexing rule should not be indexed.
         * @return this builder.
         */
        public Builder withNotIndexed() {
            this.notIndexed = true;
            return this;
        }
        
        /** Set a default value for this indexing rule.
         * @param value the default value.
         * @return this builder.
         */
        public Builder withNullableDefaultValue(final Object value) {
            this.defaultValue = value;
            return this;
        }
        
        /** Set the ui name for this indexing rule. If the ui name is not provided, it is
         * created by capitalizing the first character of the key name. Nulls and whitespace
         * strings are ignored.
         * @param uiName the ui name.
         * @return this builder.
         */
        public Builder withNullableUIName(final String uiName) {
            this.uiName = checkString(uiName);
            return this;
        }
        
        private String checkString(final String s) {
            return Utils.isNullOrEmpty(s) ? null : s;
        }

        /** Specify that the value associated with the indexing rule should not be displayed in
         * a UI.
         * @return this builder.
         */
        public Builder withUIHidden() {
            this.uiHidden = true;
            return this;
        }
        
        /** Specify that the source key or path should be applied to extract a value from the
         * parent object of a sub object, rather than from the sub object.
         * @return this builder.
         */
        public Builder withFromParent() {
            this.fromParent = true;
            return this;
        }
        
        //TODO IDXRULE must the target indexing rule be a GUID transform?
        /** Specify that the value associated with this indexing rule should be used as the text
         * for a link to a data object specified by a GUID associated with another indexing rule
         * which is specified by uiLinkKey. Nulls and whitespace strings are ignored.
         * @param uiLinkKey the source key of the {@link IndexingRules} containing a GUID that 
         * is the address of the data object that is the target of the link.
         * @return this builder.
         */
        public Builder withNullableUILinkKey(final String uiLinkKey) {
            this.uiLinkKey = checkString(uiLinkKey);
            return this;
        }
        
        /** Build the {@link IndexingRules}.
         * @return the rules.
         */
        public IndexingRules build() {
            return new IndexingRules(path, fullText, keywordType, keyName,
                    transform, fromParent, notIndexed, sourceKey,
                    defaultValue, uiName, uiHidden, uiLinkKey);
        }
        
    }
}
