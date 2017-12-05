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
    
    //TODO JAVADOC
    //TODO TEST

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
    private final String keywordType;
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
    // TODO NNOW this should point to another indexing rule that is a guid.
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
    
    //TODO NNOW return optionals instead of nulls
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
        this.keywordType = keywordType;
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

    public Optional<ObjectJsonPath> getPath() {
        return path;
    }
    
    public boolean isFullText() {
        return fullText;
    }
    
    public String getKeywordType() {
        return keywordType;
    }
    
    public String getKeyName() {
        return keyName;
    }
    
    public Optional<Transform> getTransform() {
        return transform;
    }
    
    public boolean isFromParent() {
        return fromParent;
    }
    
    public boolean isDerivedKey() {
        return sourceKey.isPresent();
    }
    
    public boolean isNotIndexed() {
        return notIndexed;
    }
    
    public Optional<String> getSourceKey() {
        return sourceKey;
    }
    
    public Optional<Object> getDefaultValue() {
        return defaultValue;
    }
    
    public String getUiName() {
        return uiName;
    }
    
    public boolean isUiHidden() {
        return uiHidden;
    }
    
    public Optional<String> getUiLinkKey() {
        return uiLinkKey;
    }
    
    @Override
    public String toString() {
        return "IndexingRules [path=" + path + ", fullText=" + fullText
                + ", keywordType=" + keywordType + ", keyName=" + keyName
                + ", transform=" + transform + ", fromParent=" + fromParent
                + ", notIndexed=" + notIndexed
                + ", sourceKey=" + sourceKey
                + ", defaultValue=" + defaultValue
                + ", uiName=" + uiName
                + ", uiHidden=" + uiHidden + ", uiLinkKey=" + uiLinkKey + "]";
    }
    
    public static Builder fromPath(final ObjectJsonPath path) {
        return new Builder(path);
    }
    
    public static Builder fromSourceKey(final String sourceKey, final String keyName) {
        return new Builder(sourceKey, keyName);
    }
    
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
        
        public Builder withKeyName(final String keyName) {
            Utils.notNullOrEmpty(keyName, "keyName cannot be null or whitespace");
            this.keyName = keyName;
            return this;
        }
        
        public Builder withFullText() {
            this.keywordType = null;
            this.fullText = true;
            return this;
        }
        
        public Builder withKeywordType(final String keywordType) {
            Utils.notNullOrEmpty(keywordType, "keywordType cannot be null or whitespace");
            this.keywordType = keywordType;
            this.fullText = false;
            return this;
        }
        
        public Builder withTransform(final Transform transform) {
            Utils.nonNull(transform, "transform");
            this.transform = transform;
            return this;
        }
        
        public Builder withNotIndexed() {
            this.notIndexed = true;
            return this;
        }
        
        public Builder withNullableDefaultValue(final Object value) {
            this.defaultValue = value;
            return this;
        }
        
        public Builder withNullableUIName(final String uiName) {
            this.uiName = checkString(uiName);
            return this;
        }
        
        private String checkString(final String s) {
            return Utils.isNullOrEmpty(s) ? null : s;
        }

        public Builder withUIHidden() {
            this.uiHidden = true;
            return this;
        }
        
        public Builder withFromParent() {
            this.fromParent = true;
            return this;
        }
        
        public Builder withNullableUILinkKey(final String uiLinkKey) {
            this.uiLinkKey = checkString(uiLinkKey);
            return this;
        }
        
        public IndexingRules build() {
            return new IndexingRules(path, fullText, keywordType, keyName,
                    transform, fromParent, notIndexed, sourceKey,
                    defaultValue, uiName, uiHidden, uiLinkKey);
        }
        
    }
}
