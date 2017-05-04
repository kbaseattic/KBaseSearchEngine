
package kbaserelationengine;

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
 * <p>Original spec-file type: SearchObjectsInput</p>
 * 
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "object_type",
    "match_filter",
    "sorting_rules",
    "access_filter",
    "pagination",
    "post_processing"
})
public class SearchObjectsInput {

    @JsonProperty("object_type")
    private String objectType;
    /**
     * <p>Original spec-file type: MatchFilter</p>
     * 
     * 
     */
    @JsonProperty("match_filter")
    private MatchFilter matchFilter;
    @JsonProperty("sorting_rules")
    private List<SortingRule> sortingRules;
    /**
     * <p>Original spec-file type: AccessFilter</p>
     * 
     * 
     */
    @JsonProperty("access_filter")
    private AccessFilter accessFilter;
    /**
     * <p>Original spec-file type: Pagination</p>
     * 
     * 
     */
    @JsonProperty("pagination")
    private Pagination pagination;
    /**
     * <p>Original spec-file type: PostProcessing</p>
     * <pre>
     * ids_only - shortcut to mark all three skips as true.
     * </pre>
     * 
     */
    @JsonProperty("post_processing")
    private PostProcessing postProcessing;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("object_type")
    public String getObjectType() {
        return objectType;
    }

    @JsonProperty("object_type")
    public void setObjectType(String objectType) {
        this.objectType = objectType;
    }

    public SearchObjectsInput withObjectType(String objectType) {
        this.objectType = objectType;
        return this;
    }

    /**
     * <p>Original spec-file type: MatchFilter</p>
     * 
     * 
     */
    @JsonProperty("match_filter")
    public MatchFilter getMatchFilter() {
        return matchFilter;
    }

    /**
     * <p>Original spec-file type: MatchFilter</p>
     * 
     * 
     */
    @JsonProperty("match_filter")
    public void setMatchFilter(MatchFilter matchFilter) {
        this.matchFilter = matchFilter;
    }

    public SearchObjectsInput withMatchFilter(MatchFilter matchFilter) {
        this.matchFilter = matchFilter;
        return this;
    }

    @JsonProperty("sorting_rules")
    public List<SortingRule> getSortingRules() {
        return sortingRules;
    }

    @JsonProperty("sorting_rules")
    public void setSortingRules(List<SortingRule> sortingRules) {
        this.sortingRules = sortingRules;
    }

    public SearchObjectsInput withSortingRules(List<SortingRule> sortingRules) {
        this.sortingRules = sortingRules;
        return this;
    }

    /**
     * <p>Original spec-file type: AccessFilter</p>
     * 
     * 
     */
    @JsonProperty("access_filter")
    public AccessFilter getAccessFilter() {
        return accessFilter;
    }

    /**
     * <p>Original spec-file type: AccessFilter</p>
     * 
     * 
     */
    @JsonProperty("access_filter")
    public void setAccessFilter(AccessFilter accessFilter) {
        this.accessFilter = accessFilter;
    }

    public SearchObjectsInput withAccessFilter(AccessFilter accessFilter) {
        this.accessFilter = accessFilter;
        return this;
    }

    /**
     * <p>Original spec-file type: Pagination</p>
     * 
     * 
     */
    @JsonProperty("pagination")
    public Pagination getPagination() {
        return pagination;
    }

    /**
     * <p>Original spec-file type: Pagination</p>
     * 
     * 
     */
    @JsonProperty("pagination")
    public void setPagination(Pagination pagination) {
        this.pagination = pagination;
    }

    public SearchObjectsInput withPagination(Pagination pagination) {
        this.pagination = pagination;
        return this;
    }

    /**
     * <p>Original spec-file type: PostProcessing</p>
     * <pre>
     * ids_only - shortcut to mark all three skips as true.
     * </pre>
     * 
     */
    @JsonProperty("post_processing")
    public PostProcessing getPostProcessing() {
        return postProcessing;
    }

    /**
     * <p>Original spec-file type: PostProcessing</p>
     * <pre>
     * ids_only - shortcut to mark all three skips as true.
     * </pre>
     * 
     */
    @JsonProperty("post_processing")
    public void setPostProcessing(PostProcessing postProcessing) {
        this.postProcessing = postProcessing;
    }

    public SearchObjectsInput withPostProcessing(PostProcessing postProcessing) {
        this.postProcessing = postProcessing;
        return this;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperties(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Override
    public String toString() {
        return ((((((((((((((("SearchObjectsInput"+" [objectType=")+ objectType)+", matchFilter=")+ matchFilter)+", sortingRules=")+ sortingRules)+", accessFilter=")+ accessFilter)+", pagination=")+ pagination)+", postProcessing=")+ postProcessing)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
