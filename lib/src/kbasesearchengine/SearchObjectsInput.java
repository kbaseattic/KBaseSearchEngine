
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
 * <p>Original spec-file type: SearchObjectsInput</p>
 * <pre>
 * Input parameters for 'search_objects' method.
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "object_types",
    "match_filter",
    "sorting_rules",
    "access_filter",
    "pagination",
    "post_processing"
})
public class SearchObjectsInput {

    @JsonProperty("object_types")
    private List<String> objectTypes;
    /**
     * <p>Original spec-file type: MatchFilter</p>
     * <pre>
     * Optional rules of defining constrains for object properties
     * including values of keywords or metadata/system properties (like
     * object name, creation time range) or full-text search in all
     * properties.
     * boolean exclude_subobjects - don't return any subobjects in the search results if true.
     *     Default false.
     * </pre>
     * 
     */
    @JsonProperty("match_filter")
    private MatchFilter matchFilter;
    @JsonProperty("sorting_rules")
    private List<SortingRule> sortingRules;
    /**
     * <p>Original spec-file type: AccessFilter</p>
     * <pre>
     * Optional rules of access constrains.
     *   - with_private - include data found in workspaces not marked 
     *       as public, default value is true,
     *   - with_public - include data found in public workspaces,
     *       default value is false,
     *   - with_all_history - include all versions (last one and all
     *       old versions) of objects matching constrains, default
     *       value is false.
     * </pre>
     * 
     */
    @JsonProperty("access_filter")
    private AccessFilter accessFilter;
    /**
     * <p>Original spec-file type: Pagination</p>
     * <pre>
     * Pagination rules. Default values are: start = 0, count = 50.
     * </pre>
     * 
     */
    @JsonProperty("pagination")
    private Pagination pagination;
    /**
     * <p>Original spec-file type: PostProcessing</p>
     * <pre>
     * Rules for what to return about found objects.
     * skip_info - do not include brief info for object ('guid,
     *     'parent_guid', 'object_name' and 'timestamp' fields in
     *     ObjectData structure),
     * skip_keys - do not include keyword values for object 
     *     ('key_props' field in ObjectData structure),
     * skip_data - do not include raw data for object ('data' and 
     *     'parent_data' fields in ObjectData structure),
     * ids_only - shortcut to mark all three skips as true.
     * </pre>
     * 
     */
    @JsonProperty("post_processing")
    private PostProcessing postProcessing;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("object_types")
    public List<String> getObjectTypes() {
        return objectTypes;
    }

    @JsonProperty("object_types")
    public void setObjectTypes(List<String> objectTypes) {
        this.objectTypes = objectTypes;
    }

    public SearchObjectsInput withObjectTypes(List<String> objectTypes) {
        this.objectTypes = objectTypes;
        return this;
    }

    /**
     * <p>Original spec-file type: MatchFilter</p>
     * <pre>
     * Optional rules of defining constrains for object properties
     * including values of keywords or metadata/system properties (like
     * object name, creation time range) or full-text search in all
     * properties.
     * boolean exclude_subobjects - don't return any subobjects in the search results if true.
     *     Default false.
     * </pre>
     * 
     */
    @JsonProperty("match_filter")
    public MatchFilter getMatchFilter() {
        return matchFilter;
    }

    /**
     * <p>Original spec-file type: MatchFilter</p>
     * <pre>
     * Optional rules of defining constrains for object properties
     * including values of keywords or metadata/system properties (like
     * object name, creation time range) or full-text search in all
     * properties.
     * boolean exclude_subobjects - don't return any subobjects in the search results if true.
     *     Default false.
     * </pre>
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
     * <pre>
     * Optional rules of access constrains.
     *   - with_private - include data found in workspaces not marked 
     *       as public, default value is true,
     *   - with_public - include data found in public workspaces,
     *       default value is false,
     *   - with_all_history - include all versions (last one and all
     *       old versions) of objects matching constrains, default
     *       value is false.
     * </pre>
     * 
     */
    @JsonProperty("access_filter")
    public AccessFilter getAccessFilter() {
        return accessFilter;
    }

    /**
     * <p>Original spec-file type: AccessFilter</p>
     * <pre>
     * Optional rules of access constrains.
     *   - with_private - include data found in workspaces not marked 
     *       as public, default value is true,
     *   - with_public - include data found in public workspaces,
     *       default value is false,
     *   - with_all_history - include all versions (last one and all
     *       old versions) of objects matching constrains, default
     *       value is false.
     * </pre>
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
     * <pre>
     * Pagination rules. Default values are: start = 0, count = 50.
     * </pre>
     * 
     */
    @JsonProperty("pagination")
    public Pagination getPagination() {
        return pagination;
    }

    /**
     * <p>Original spec-file type: Pagination</p>
     * <pre>
     * Pagination rules. Default values are: start = 0, count = 50.
     * </pre>
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
     * Rules for what to return about found objects.
     * skip_info - do not include brief info for object ('guid,
     *     'parent_guid', 'object_name' and 'timestamp' fields in
     *     ObjectData structure),
     * skip_keys - do not include keyword values for object 
     *     ('key_props' field in ObjectData structure),
     * skip_data - do not include raw data for object ('data' and 
     *     'parent_data' fields in ObjectData structure),
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
     * Rules for what to return about found objects.
     * skip_info - do not include brief info for object ('guid,
     *     'parent_guid', 'object_name' and 'timestamp' fields in
     *     ObjectData structure),
     * skip_keys - do not include keyword values for object 
     *     ('key_props' field in ObjectData structure),
     * skip_data - do not include raw data for object ('data' and 
     *     'parent_data' fields in ObjectData structure),
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
        return ((((((((((((((("SearchObjectsInput"+" [objectTypes=")+ objectTypes)+", matchFilter=")+ matchFilter)+", sortingRules=")+ sortingRules)+", accessFilter=")+ accessFilter)+", pagination=")+ pagination)+", postProcessing=")+ postProcessing)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
