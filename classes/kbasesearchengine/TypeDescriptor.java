
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
 * <p>Original spec-file type: TypeDescriptor</p>
 * <pre>
 * Description of searchable object type including details about keywords.
 * TODO: add more details like parent type, relations, primary key, ... (relation engine stuff)
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "type_name",
    "type_ui_title",
    "keys"
})
public class TypeDescriptor {

    @JsonProperty("type_name")
    private String typeName;
    @JsonProperty("type_ui_title")
    private String typeUiTitle;
    @JsonProperty("keys")
    private List<KeyDescription> keys;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("type_name")
    public String getTypeName() {
        return typeName;
    }

    @JsonProperty("type_name")
    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public TypeDescriptor withTypeName(String typeName) {
        this.typeName = typeName;
        return this;
    }

    @JsonProperty("type_ui_title")
    public String getTypeUiTitle() {
        return typeUiTitle;
    }

    @JsonProperty("type_ui_title")
    public void setTypeUiTitle(String typeUiTitle) {
        this.typeUiTitle = typeUiTitle;
    }

    public TypeDescriptor withTypeUiTitle(String typeUiTitle) {
        this.typeUiTitle = typeUiTitle;
        return this;
    }

    @JsonProperty("keys")
    public List<KeyDescription> getKeys() {
        return keys;
    }

    @JsonProperty("keys")
    public void setKeys(List<KeyDescription> keys) {
        this.keys = keys;
    }

    public TypeDescriptor withKeys(List<KeyDescription> keys) {
        this.keys = keys;
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
        return ((((((((("TypeDescriptor"+" [typeName=")+ typeName)+", typeUiTitle=")+ typeUiTitle)+", keys=")+ keys)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
