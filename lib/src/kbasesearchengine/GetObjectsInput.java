
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
 * <p>Original spec-file type: GetObjectsInput</p>
 * <pre>
 * Input parameters for get_objects method.
 *     guids - list of guids
 *     post_processing - see PostProcessing (optional).
 *     match_filter - see MatchFilter (optional).
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "guids",
    "post_processing",
    "match_filter"
})
public class GetObjectsInput {

    @JsonProperty("guids")
    private List<String> guids;
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
    private Map<java.lang.String, Object> additionalProperties = new HashMap<java.lang.String, Object>();

    @JsonProperty("guids")
    public List<String> getGuids() {
        return guids;
    }

    @JsonProperty("guids")
    public void setGuids(List<String> guids) {
        this.guids = guids;
    }

    public GetObjectsInput withGuids(List<String> guids) {
        this.guids = guids;
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

    public GetObjectsInput withPostProcessing(PostProcessing postProcessing) {
        this.postProcessing = postProcessing;
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

    public GetObjectsInput withMatchFilter(MatchFilter matchFilter) {
        this.matchFilter = matchFilter;
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
        return ((((((((("GetObjectsInput"+" [guids=")+ guids)+", postProcessing=")+ postProcessing)+", matchFilter=")+ matchFilter)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
