
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
 * <p>Original spec-file type: GetObjectsOutput</p>
 * 
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "objects",
    "search_time"
})
public class GetObjectsOutput {

    @JsonProperty("objects")
    private List<ObjectData> objects;
    @JsonProperty("search_time")
    private Long searchTime;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

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
        return ((((((("GetObjectsOutput"+" [objects=")+ objects)+", searchTime=")+ searchTime)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
