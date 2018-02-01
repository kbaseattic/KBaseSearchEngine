
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
 * <p>Original spec-file type: GetObjectsOutput</p>
 * <pre>
 * Output results of get_objects method.
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "objects",
    "search_time",
    "ws_narrative_info"
})
public class GetObjectsOutput {

    @JsonProperty("objects")
    private List<ObjectData> objects;
    @JsonProperty("search_time")
    private Long searchTime;
    @JsonProperty("ws_narrative_info")
    private Map<String, NarrativeInfo> wsNarrativeInfo;
    private Map<java.lang.String, Object> additionalProperties = new HashMap<java.lang.String, Object>();

    @JsonProperty("objects")
    public List<ObjectData> getObjects() {
        return objects;
    }

    @JsonProperty("objects")
    public void setObjects(List<ObjectData> objects) {
        this.objects = objects;
    }

    public GetObjectsOutput withObjects(List<ObjectData> objects) {
        this.objects = objects;
        return this;
    }

    @JsonProperty("search_time")
    public Long getSearchTime() {
        return searchTime;
    }

    @JsonProperty("search_time")
    public void setSearchTime(Long searchTime) {
        this.searchTime = searchTime;
    }

    public GetObjectsOutput withSearchTime(Long searchTime) {
        this.searchTime = searchTime;
        return this;
    }

    @JsonProperty("ws_narrative_info")
    public Map<String, NarrativeInfo> getWsNarrativeInfo() {
        return wsNarrativeInfo;
    }

    @JsonProperty("ws_narrative_info")
    public void setWsNarrativeInfo(Map<String, NarrativeInfo> wsNarrativeInfo) {
        this.wsNarrativeInfo = wsNarrativeInfo;
    }

    public GetObjectsOutput withWsNarrativeInfo(Map<String, NarrativeInfo> wsNarrativeInfo) {
        this.wsNarrativeInfo = wsNarrativeInfo;
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
        return ((((((((("GetObjectsOutput"+" [objects=")+ objects)+", searchTime=")+ searchTime)+", wsNarrativeInfo=")+ wsNarrativeInfo)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
