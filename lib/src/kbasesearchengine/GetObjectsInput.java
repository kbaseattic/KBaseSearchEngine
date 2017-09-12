
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
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "guids",
    "post_processing"
})
public class GetObjectsInput {

    @JsonProperty("guids")
    private List<String> guids;
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

    public GetObjectsInput withPostProcessing(PostProcessing postProcessing) {
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
        return ((((((("GetObjectsInput"+" [guids=")+ guids)+", postProcessing=")+ postProcessing)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
