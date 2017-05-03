
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
 * <p>Original spec-file type: ListTypeKeysOutput</p>
 * 
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "type_to_keys"
})
public class ListTypeKeysOutput {

    @JsonProperty("type_to_keys")
    private Map<String, List<KeyDescription>> typeToKeys;
    private Map<java.lang.String, Object> additionalProperties = new HashMap<java.lang.String, Object>();

    @JsonProperty("type_to_keys")
    public Map<String, List<KeyDescription>> getTypeToKeys() {
        return typeToKeys;
    }

    @JsonProperty("type_to_keys")
    public void setTypeToKeys(Map<String, List<KeyDescription>> typeToKeys) {
        this.typeToKeys = typeToKeys;
    }

    public ListTypeKeysOutput withTypeToKeys(Map<String, List<KeyDescription>> typeToKeys) {
        this.typeToKeys = typeToKeys;
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
        return ((((("ListTypeKeysOutput"+" [typeToKeys=")+ typeToKeys)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
