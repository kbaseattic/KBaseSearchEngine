
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
 * <p>Original spec-file type: SortingRule</p>
 * <pre>
 * Rule for sorting results. 
 * string property - the property to sort on. This may be a an object property - e.g. a 
 *     field inside the object - or a standard property possessed by all objects, like a
 *     timestamp or creator.
 * boolean is_object_property - true (the default) to specify an object property, false to
 *     specify a standard property.
 * boolean ascending - true (the default) to sort ascending, false to sort descending.
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "property",
    "is_object_property",
    "ascending"
})
public class SortingRule {

    @JsonProperty("property")
    private String property;
    @JsonProperty("is_object_property")
    private Long isObjectProperty;
    @JsonProperty("ascending")
    private Long ascending;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("property")
    public String getProperty() {
        return property;
    }

    @JsonProperty("property")
    public void setProperty(String property) {
        this.property = property;
    }

    public SortingRule withProperty(String property) {
        this.property = property;
        return this;
    }

    @JsonProperty("is_object_property")
    public Long getIsObjectProperty() {
        return isObjectProperty;
    }

    @JsonProperty("is_object_property")
    public void setIsObjectProperty(Long isObjectProperty) {
        this.isObjectProperty = isObjectProperty;
    }

    public SortingRule withIsObjectProperty(Long isObjectProperty) {
        this.isObjectProperty = isObjectProperty;
        return this;
    }

    @JsonProperty("ascending")
    public Long getAscending() {
        return ascending;
    }

    @JsonProperty("ascending")
    public void setAscending(Long ascending) {
        this.ascending = ascending;
    }

    public SortingRule withAscending(Long ascending) {
        this.ascending = ascending;
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
        return ((((((((("SortingRule"+" [property=")+ property)+", isObjectProperty=")+ isObjectProperty)+", ascending=")+ ascending)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
