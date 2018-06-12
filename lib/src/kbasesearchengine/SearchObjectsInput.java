
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
 * object_types - list of the types of objects to search on (optional). The
 *                function will search on all objects if the list is not specified
 *                or is empty. The list size must be less than 50.
 * match_filter - see MatchFilter.
 * sorting_rules - see SortingRule (optional).
 * access_filter - see AccessFilter.
 * pagination - see Pagination (optional).
 * post_processing - see PostProcessing (optional).
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
    @JsonProperty("match_filter")
    private MatchFilter matchFilter;
    @JsonProperty("sorting_rules")
    private List<SortingRule> sortingRules;
    /**
     * <p>Original spec-file type: AccessFilter</p>
     * <pre>
     * Optional rules of access constraints.
     *   - with_private - include data found in workspaces not marked 
     *               as public, default value is true for authenticated users and false for unauthenticated users.
     *   - with_public - include data found in public workspaces,
     *               default value is false for authenticated users and true for unauthenticated users.
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
     * skip_keys - do not include keyword values for object 
     *     ('key_props' field in ObjectData structure),
     * skip_data - do not include raw data for object ('data' and 
     *     'parent_data' fields in ObjectData structure),
     * include_highlight - include highlights of fields that
     *      matched query,
     * ids_only - shortcut to mark both skips as true and 
     *      include_highlight as false.
     * add_narrative_info - if true, narrative info gets added to the
     *      search results. Default is false.
     * add_access_group_info - if true, access groups and objects info get added
     *      to the search results. Default is false.
     * </pre>
     * 
     */
    @JsonProperty("post_processing")
    private PostProcessing postProcessing;
    private Map<java.lang.String, Object> additionalProperties = new HashMap<java.lang.String, Object>();

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
     * Optional rules of access constraints.
     *   - with_private - include data found in workspaces not marked 
     *               as public, default value is true for authenticated users and false for unauthenticated users.
     *   - with_public - include data found in public workspaces,
     *               default value is false for authenticated users and true for unauthenticated users.
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
     * Optional rules of access constraints.
     *   - with_private - include data found in workspaces not marked 
     *               as public, default value is true for authenticated users and false for unauthenticated users.
     *   - with_public - include data found in public workspaces,
     *               default value is false for authenticated users and true for unauthenticated users.
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
     * skip_keys - do not include keyword values for object 
     *     ('key_props' field in ObjectData structure),
     * skip_data - do not include raw data for object ('data' and 
     *     'parent_data' fields in ObjectData structure),
     * include_highlight - include highlights of fields that
     *      matched query,
     * ids_only - shortcut to mark both skips as true and 
     *      include_highlight as false.
     * add_narrative_info - if true, narrative info gets added to the
     *      search results. Default is false.
     * add_access_group_info - if true, access groups and objects info get added
     *      to the search results. Default is false.
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
     * skip_keys - do not include keyword values for object 
     *     ('key_props' field in ObjectData structure),
     * skip_data - do not include raw data for object ('data' and 
     *     'parent_data' fields in ObjectData structure),
     * include_highlight - include highlights of fields that
     *      matched query,
     * ids_only - shortcut to mark both skips as true and 
     *      include_highlight as false.
     * add_narrative_info - if true, narrative info gets added to the
     *      search results. Default is false.
     * add_access_group_info - if true, access groups and objects info get added
     *      to the search results. Default is false.
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
    public Map<java.lang.String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperties(java.lang.String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Override
    public java.lang.String toString() {
        return ((((((((((((((("SearchObjectsInput"+" [objectTypes=")+ objectTypes)+", matchFilter=")+ matchFilter)+", sortingRules=")+ sortingRules)+", accessFilter=")+ accessFilter)+", pagination=")+ pagination)+", postProcessing=")+ postProcessing)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
