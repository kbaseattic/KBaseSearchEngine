
package kbaserelationengine;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;


/**
 * <p>Original spec-file type: SearchTypesInput</p>
 * 
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "match_filter",
    "access_filter"
})
public class SearchTypesInput {

    /**
     * <p>Original spec-file type: MatchFilter</p>
     * 
     * 
     */
    @JsonProperty("match_filter")
    private MatchFilter matchFilter;
    /**
     * <p>Original spec-file type: AccessFilter</p>
     * 
     * 
     */
    @JsonProperty("access_filter")
    private AccessFilter accessFilter;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

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

    public SearchTypesInput withMatchFilter(MatchFilter matchFilter) {
        this.matchFilter = matchFilter;
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

    public SearchTypesInput withAccessFilter(AccessFilter accessFilter) {
        this.accessFilter = accessFilter;
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
        return ((((((("SearchTypesInput"+" [matchFilter=")+ matchFilter)+", accessFilter=")+ accessFilter)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
