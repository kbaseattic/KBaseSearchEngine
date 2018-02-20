
package kbasesearchengine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;


/**
 * <p>Original spec-file type: MatchFilter</p>
 * <pre>
 * Optional rules of defining constrains for object properties
 * including values of keywords or metadata/system properties (like
 * object name, creation time range) or full-text search in all
 * properties.
 * boolean exclude_subobjects - don't return any subobjects in the search results if true.
 *     Default false.
 * list<string> source_tags - source tags are arbitrary strings applied to data at the data
 *     source (for example, the workspace service). The source_tags list may optionally be
 *     populated with a set of tags that will determine what data is returned in a search.
 *     By default, the list behaves as a whitelist and only data with at least one of the
 *     tags will be returned.
 * source_tags_blacklist - if true, the source_tags list behaves as a blacklist and any
 *     data with at least one of the tags will be excluded from the search results. If missing
 *     or false, the default behavior is maintained.
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "full_text_in_all",
    "object_name",
    "timestamp",
    "exclude_subobjects",
    "lookupInKeys",
    "source_tags",
    "source_tags_blacklist"
})
public class MatchFilter {

    @JsonProperty("full_text_in_all")
    private java.lang.String fullTextInAll;
    @JsonProperty("object_name")
    private java.lang.String objectName;
    /**
     * <p>Original spec-file type: MatchValue</p>
     * <pre>
     * Optional rules of defining constraints for values of particular
     * term (keyword). Appropriate field depends on type of keyword.
     * For instance in case of integer type 'int_value' should be used.
     * In case of range constraint rather than single value 'min_*' 
     * and 'max_*' fields should be used. You may omit one of ends of
     * range to achieve '<=' or '>=' comparison. Ends are always
     * included for range constraints.
     * </pre>
     * 
     */
    @JsonProperty("timestamp")
    private kbasesearchengine.MatchValue timestamp;
    @JsonProperty("exclude_subobjects")
    private Long excludeSubobjects;
    @JsonProperty("lookupInKeys")
    private Map<String, kbasesearchengine.MatchValue> lookupInKeys;
    @JsonProperty("source_tags")
    private List<String> sourceTags;
    @JsonProperty("source_tags_blacklist")
    private Long sourceTagsBlacklist;
    private Map<java.lang.String, Object> additionalProperties = new HashMap<java.lang.String, Object>();

    @JsonProperty("full_text_in_all")
    public java.lang.String getFullTextInAll() {
        return fullTextInAll;
    }

    @JsonProperty("full_text_in_all")
    public void setFullTextInAll(java.lang.String fullTextInAll) {
        this.fullTextInAll = fullTextInAll;
    }

    public MatchFilter withFullTextInAll(java.lang.String fullTextInAll) {
        this.fullTextInAll = fullTextInAll;
        return this;
    }

    @JsonProperty("object_name")
    public java.lang.String getObjectName() {
        return objectName;
    }

    @JsonProperty("object_name")
    public void setObjectName(java.lang.String objectName) {
        this.objectName = objectName;
    }

    public MatchFilter withObjectName(java.lang.String objectName) {
        this.objectName = objectName;
        return this;
    }

    /**
     * <p>Original spec-file type: MatchValue</p>
     * <pre>
     * Optional rules of defining constraints for values of particular
     * term (keyword). Appropriate field depends on type of keyword.
     * For instance in case of integer type 'int_value' should be used.
     * In case of range constraint rather than single value 'min_*' 
     * and 'max_*' fields should be used. You may omit one of ends of
     * range to achieve '<=' or '>=' comparison. Ends are always
     * included for range constraints.
     * </pre>
     * 
     */
    @JsonProperty("timestamp")
    public kbasesearchengine.MatchValue getTimestamp() {
        return timestamp;
    }

    /**
     * <p>Original spec-file type: MatchValue</p>
     * <pre>
     * Optional rules of defining constraints for values of particular
     * term (keyword). Appropriate field depends on type of keyword.
     * For instance in case of integer type 'int_value' should be used.
     * In case of range constraint rather than single value 'min_*' 
     * and 'max_*' fields should be used. You may omit one of ends of
     * range to achieve '<=' or '>=' comparison. Ends are always
     * included for range constraints.
     * </pre>
     * 
     */
    @JsonProperty("timestamp")
    public void setTimestamp(kbasesearchengine.MatchValue timestamp) {
        this.timestamp = timestamp;
    }

    public MatchFilter withTimestamp(kbasesearchengine.MatchValue timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    @JsonProperty("exclude_subobjects")
    public Long getExcludeSubobjects() {
        return excludeSubobjects;
    }

    @JsonProperty("exclude_subobjects")
    public void setExcludeSubobjects(Long excludeSubobjects) {
        this.excludeSubobjects = excludeSubobjects;
    }

    public MatchFilter withExcludeSubobjects(Long excludeSubobjects) {
        this.excludeSubobjects = excludeSubobjects;
        return this;
    }

    @JsonProperty("lookupInKeys")
    public Map<String, kbasesearchengine.MatchValue> getLookupInKeys() {
        return lookupInKeys;
    }

    @JsonProperty("lookupInKeys")
    public void setLookupInKeys(Map<String, kbasesearchengine.MatchValue> lookupInKeys) {
        this.lookupInKeys = lookupInKeys;
    }

    public MatchFilter withLookupInKeys(Map<String, kbasesearchengine.MatchValue> lookupInKeys) {
        this.lookupInKeys = lookupInKeys;
        return this;
    }

    @JsonProperty("source_tags")
    public List<String> getSourceTags() {
        return sourceTags;
    }

    @JsonProperty("source_tags")
    public void setSourceTags(List<String> sourceTags) {
        this.sourceTags = sourceTags;
    }

    public MatchFilter withSourceTags(List<String> sourceTags) {
        this.sourceTags = sourceTags;
        return this;
    }

    @JsonProperty("source_tags_blacklist")
    public Long getSourceTagsBlacklist() {
        return sourceTagsBlacklist;
    }

    @JsonProperty("source_tags_blacklist")
    public void setSourceTagsBlacklist(Long sourceTagsBlacklist) {
        this.sourceTagsBlacklist = sourceTagsBlacklist;
    }

    public MatchFilter withSourceTagsBlacklist(Long sourceTagsBlacklist) {
        this.sourceTagsBlacklist = sourceTagsBlacklist;
        return this;
    }

    @JsonAnyGetter
    public Map<java.lang.String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperties(java.lang.String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Override
    public java.lang.String toString() {
        return ((((((((((((((((("MatchFilter"+" [fullTextInAll=")+ fullTextInAll)+", objectName=")+ objectName)+", timestamp=")+ timestamp)+", excludeSubobjects=")+ excludeSubobjects)+", lookupInKeys=")+ lookupInKeys)+", sourceTags=")+ sourceTags)+", sourceTagsBlacklist=")+ sourceTagsBlacklist)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
