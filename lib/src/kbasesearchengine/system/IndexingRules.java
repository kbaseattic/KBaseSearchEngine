package kbasesearchengine.system;

import kbasesearchengine.common.ObjectJsonPath;

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
 * See {@link #validate()} for instructions on building unambigious indexing rules.
 *
 */
public class IndexingRules {
    // supported transformations
    public static final String TRANSFORM_LOCATION = "location";
    public static final String TRANSFORM_LENGTH = "length";
    public static final String TRANSFORM_VALUES = "values";
    public static final String TRANSFORM_STRING = "string";
    public static final String TRANSFORM_INTEGER = "integer";
    public static final String TRANSFORM_GUID = "guid";
    public static final String TRANSFORM_LOOKUP = "lookup";

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
    private ObjectJsonPath path = null;
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
    private boolean fullText = false;
    /**
     * The "keyword" type to use in ElasticSearch. This value must be specified
     * if fullText is set to false. Example: "integer", "string" etc.
     *
     * Defaults to "keyword" type in ElasticSearch if not defined and
     * fullText=false.
     *
     */
    private String keywordType = null;
    /**
     * Key name for keyword. If it is not specified, then first item between
     * slashes in "path" is used.
     *
     */
    private String keyName = null;
    /** An optional transformation applied to values coming from a [sub-]object
     * or source keyword. The value of this property has the format of
     * <transform>[.<ret-prop>], where second part (including dot) is optional
     * and used in some of transformations.
     *
     */
    private String transform = null;
    /**
     * An optional attribute that indicates that the value is extracted from the
     * parent object rather that from the sub-object.
     *
     */
    private boolean fromParent = false;
    /**
     * An optional flag indicating that this rule defines a keyword formed by
     * another (source) keyword which is set in "source-key".
     *
     */
    private boolean derivedKey = false;
    /**
     * notIndexed=true indicates that value should be included into extracted part
     * of object data but must not be present as indexed keyword.
     *
     */
    private boolean notIndexed = false;
    /**
     * An optional flag indicating that this rule defines a keyword formed by
     * another (source) keyword which is set in "source-key".
     *
     */
    private String sourceKey = null;

    /** An optional attribute that can be defined for derived keywords only.
     * It is used in "guid" transform mode only. This property provides the type
     * descriptor name of [sub-]object where resulting GUID points to. In
     * addition to the validation purpose target type helps form sub-object part
     * of GUID where we need to know not only sub-object ID
     * (coming from "subobject-id-key") but also sub-object inner type which
     * is extracted from this target type descriptor.
     *
     */
    private String targetObjectType = null;
    /**
     * An optional attribute that can be defined for derived keywords only.
     * Works together with "target-object-type". Is used in "guid" transform mode
     * only. This property points to keywords providing value for sub-object
     * inner ID in order to construct full GUID for sub-object.
     *
     */
    private String subobjectIdKey = null;
    /**
     * An optional value which is used for the keyword in case the resulting
     * array of values extracted from the document [sub-]object is empty.
     */
    private Object optionalDefaultValue = null;
    /**
     * TODO figure this one out
     */
    private Object constantValue = null;
    /**
     * Name of keyword displayed by UI.
     */
    private String uiName = null;
    /**
     * An optional attribute that indicates that a particular keyword is not
     * supposed to be visible via UI though it could be used in API search queries.
     */
    private boolean uiHidden = false;
    /**
     * An optional pointer to a paired keyword coupled with given one providing
     * GUID for making clickable URL for value provided.
     * Example: one keyword called "Genome name" may have "ui-link-key" pointer
     * to another keyword called "Genome GUID" so that Search UI will use
     * values of these two keywords in order to produce clickable link
     * showing genome name (coming from one keyword) and redirecting you to
     * landing page of given genome based on GUID (coming from another keyword).
     */
    private String uiLinkKey = null;

    /** Checks if this indexing rule is valid.
     *
     * @throws ValidationException if these rules are found to be invalid.
     */
    public void validate() throws ValidationException {
        if (!derivedKey && path == null) {
            throw new ValidationException("Must specify either derivedKey=true " +
                    "and source key, or non-null path to form keyword from: " +
                    toString());
        }
        if (derivedKey && path != null) {
            throw new ValidationException("Specify either derivedKey=true or " +
                    "sourceKey or path, but not both: " + toString());
        }
        if (derivedKey && sourceKey == null) {
            throw new ValidationException("derivedKey is true and source-key is " +
                    "null expecting a non-null sourceKey to form the derived: " +
                    "keyword from. " + toString());
        }
        if (fullText && keywordType != null) {
            throw new ValidationException("Specify either fullText=true or " +
                    "sourceKey, but not both: " + toString());
        }
        if (transform != null && !( transform.startsWith("location") ||
                transform.startsWith(TRANSFORM_LENGTH) ||
                transform.startsWith(TRANSFORM_VALUES) ||
                transform.startsWith(TRANSFORM_STRING) ||
                transform.startsWith(TRANSFORM_INTEGER) ||
                transform.startsWith(TRANSFORM_GUID) ||
                transform.startsWith(TRANSFORM_LOOKUP))) {
            throw new ValidationException("Unsupported transformation. Must be " +
                    "one of location, length, values, string, integer, guid: "
                    + toString());
        }
        if (targetObjectType != null &&
                (transform == null || !transform.startsWith("guid"))) {
            throw new ValidationException("targetObjectType must be" +
                    "used with guid transform only. Transform is either null or" +
                    "not a guid transform: " + toString());
        }
        if (subobjectIdKey != null && !derivedKey) {
            throw new ValidationException("subobjectIdKey can be" +
                    "defined for derivedKey only, but derivedKey is set to " +
                    "false!: " + toString());
        }
        if (subobjectIdKey != null &&
                (transform == null || !transform.startsWith("guid"))) {
            throw new ValidationException("subobjectIdKey must be" +
                    "used with guid transform only. Transform is either null or" +
                    "not a guid transform: " + toString());
        }
    }
    
    public ObjectJsonPath getPath() {
        return path;
    }
    
    public void setPath(ObjectJsonPath path) {
        this.path = path;
    }
    
    public boolean isFullText() {
        return fullText;
    }
    
    public void setFullText(boolean fullText) {
        this.fullText = fullText;
    }

    public String getKeywordType() {
        return keywordType;
    }
    
    public void setKeywordType(String keywordType) {
        this.keywordType = keywordType;
    }
    
    public String getKeyName() {
        return keyName;
    }
    
    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }
    
    public String getTransform() {
        return transform;
    }
    
    public void setTransform(String transform) {
        this.transform = transform;
    }
    
    public boolean isFromParent() {
        return fromParent;
    }
    
    public void setFromParent(boolean fromParent) {
        this.fromParent = fromParent;
    }
    
    public boolean isDerivedKey() {
        return derivedKey;
    }
    
    public void setDerivedKey(boolean derivedKey) {
        this.derivedKey = derivedKey;
    }
    
    public boolean isNotIndexed() {
        return notIndexed;
    }
    
    public void setNotIndexed(boolean notIndexed) {
        this.notIndexed = notIndexed;
    }
    
    public String getSourceKey() {
        return sourceKey;
    }
    
    public void setSourceKey(String sourceKey) {
        this.sourceKey = sourceKey;
    }
    
    public String getTargetObjectType() {
        return targetObjectType;
    }
    
    public void setTargetObjectType(String targetObjectType) {
        this.targetObjectType = targetObjectType;
    }
    
    public String getSubobjectIdKey() {
        return subobjectIdKey;
    }
    
    public void setSubobjectIdKey(String subobjectIdKey) {
        this.subobjectIdKey = subobjectIdKey;
    }
    
    public Object getConstantValue() {
        return constantValue;
    }
    
    public void setConstantValue(Object constantValue) {
        this.constantValue = constantValue;
    }
    
    public Object getOptionalDefaultValue() {
        return optionalDefaultValue;
    }
    
    public void setOptionalDefaultValue(Object optionalDefaultValue) {
        this.optionalDefaultValue = optionalDefaultValue;
    }
    
    public String getUiName() {
        return uiName;
    }
    
    public void setUiName(String uiName) {
        this.uiName = uiName;
    }
    
    public boolean isUiHidden() {
        return uiHidden;
    }
    
    public void setUiHidden(boolean uiHidden) {
        this.uiHidden = uiHidden;
    }
    
    public String getUiLinkKey() {
        return uiLinkKey;
    }
    
    public void setUiLinkKey(String uiLinkKey) {
        this.uiLinkKey = uiLinkKey;
    }

    @Override
    public String toString() {
        return "IndexingRules [path=" + path + ", fullText=" + fullText
                + ", keywordType=" + keywordType + ", keyName=" + keyName
                + ", transform=" + transform + ", fromParent=" + fromParent
                + ", derivedKey=" + derivedKey + ", notIndexed=" + notIndexed
                + ", sourceKey=" + sourceKey + ", targetObjectType="
                + targetObjectType + ", subobjectIdKey=" + subobjectIdKey
                + ", optionalDefaultValue=" + optionalDefaultValue
                + ", constantValue=" + constantValue + ", uiName=" + uiName
                + ", uiHidden=" + uiHidden + ", uiLinkKey=" + uiLinkKey + "]";
    }
}
