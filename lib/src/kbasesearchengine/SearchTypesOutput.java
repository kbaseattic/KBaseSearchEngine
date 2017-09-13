
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
 * <p>Original spec-file type: SearchTypesOutput</p>
 * <pre>
 * Output results of search_types method.
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "type_to_count",
    "search_time"
})
public class SearchTypesOutput {

    @JsonProperty("type_to_count")
    private Map<String, Long> typeToCount;
    @JsonProperty("search_time")
    private java.lang.Long searchTime;
    private Map<java.lang.String, Object> additionalProperties = new HashMap<java.lang.String, Object>();

    @JsonProperty("type_to_count")
    public Map<String, Long> getTypeToCount() {
        return typeToCount;
    }

    @JsonProperty("type_to_count")
    public void setTypeToCount(Map<String, Long> typeToCount) {
        this.typeToCount = typeToCount;
    }

    public SearchTypesOutput withTypeToCount(Map<String, Long> typeToCount) {
        this.typeToCount = typeToCount;
        return this;
    }

    @JsonProperty("search_time")
    public java.lang.Long getSearchTime() {
        return searchTime;
    }

    @JsonProperty("search_time")
    public void setSearchTime(java.lang.Long searchTime) {
        this.searchTime = searchTime;
    }

    public SearchTypesOutput withSearchTime(java.lang.Long searchTime) {
        this.searchTime = searchTime;
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
        return ((((((("SearchTypesOutput"+" [typeToCount=")+ typeToCount)+", searchTime=")+ searchTime)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
