
package kbasesearchengine;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;


/**
 * <p>Original spec-file type: NarrativeInfo</p>
 * 
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "narrative_id",
    "narrative_name"
})
public class NarrativeInfo {

    @JsonProperty("narrative_id")
    private Long narrativeId;
    @JsonProperty("narrative_name")
    private String narrativeName;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("narrative_id")
    public Long getNarrativeId() {
        return narrativeId;
    }

    @JsonProperty("narrative_id")
    public void setNarrativeId(Long narrativeId) {
        this.narrativeId = narrativeId;
    }

    public NarrativeInfo withNarrativeId(Long narrativeId) {
        this.narrativeId = narrativeId;
        return this;
    }

    @JsonProperty("narrative_name")
    public String getNarrativeName() {
        return narrativeName;
    }

    @JsonProperty("narrative_name")
    public void setNarrativeName(String narrativeName) {
        this.narrativeName = narrativeName;
    }

    public NarrativeInfo withNarrativeName(String narrativeName) {
        this.narrativeName = narrativeName;
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
        return ((((((("NarrativeInfo"+" [narrativeId=")+ narrativeId)+", narrativeName=")+ narrativeName)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
