
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
 * <p>Original spec-file type: KeyDescription</p>
 * 
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "key_name",
    "key_ui_title",
    "key_value_type"
})
public class KeyDescription {

    @JsonProperty("key_name")
    private String keyName;
    @JsonProperty("key_ui_title")
    private String keyUiTitle;
    @JsonProperty("key_value_type")
    private String keyValueType;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("key_name")
    public String getKeyName() {
        return keyName;
    }

    @JsonProperty("key_name")
    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    public KeyDescription withKeyName(String keyName) {
        this.keyName = keyName;
        return this;
    }

    @JsonProperty("key_ui_title")
    public String getKeyUiTitle() {
        return keyUiTitle;
    }

    @JsonProperty("key_ui_title")
    public void setKeyUiTitle(String keyUiTitle) {
        this.keyUiTitle = keyUiTitle;
    }

    public KeyDescription withKeyUiTitle(String keyUiTitle) {
        this.keyUiTitle = keyUiTitle;
        return this;
    }

    @JsonProperty("key_value_type")
    public String getKeyValueType() {
        return keyValueType;
    }

    @JsonProperty("key_value_type")
    public void setKeyValueType(String keyValueType) {
        this.keyValueType = keyValueType;
    }

    public KeyDescription withKeyValueType(String keyValueType) {
        this.keyValueType = keyValueType;
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
        return ((((((((("KeyDescription"+" [keyName=")+ keyName)+", keyUiTitle=")+ keyUiTitle)+", keyValueType=")+ keyValueType)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
