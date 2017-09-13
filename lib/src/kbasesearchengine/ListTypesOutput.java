
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
 * <p>Original spec-file type: ListTypesOutput</p>
 * <pre>
 * Output results of list_types method.
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "types"
})
public class ListTypesOutput {

    @JsonProperty("types")
    private Map<String, TypeDescriptor> types;
    private Map<java.lang.String, Object> additionalProperties = new HashMap<java.lang.String, Object>();

    @JsonProperty("types")
    public Map<String, TypeDescriptor> getTypes() {
        return types;
    }

    @JsonProperty("types")
    public void setTypes(Map<String, TypeDescriptor> types) {
        this.types = types;
    }

    public ListTypesOutput withTypes(Map<String, TypeDescriptor> types) {
        this.types = types;
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
        return ((((("ListTypesOutput"+" [types=")+ types)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
