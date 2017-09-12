
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
 * Rule for sorting found results. 'key_name', 'is_timestamp' and
 * 'is_object_name' are alternative way of defining what property
 * if used for sorting. Default order is ascending (if 
 * 'descending' field is not set).
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "is_timestamp",
    "is_object_name",
    "key_name",
    "descending"
})
public class SortingRule {

    @JsonProperty("is_timestamp")
    private Long isTimestamp;
    @JsonProperty("is_object_name")
    private Long isObjectName;
    @JsonProperty("key_name")
    private String keyName;
    @JsonProperty("descending")
    private Long descending;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("is_timestamp")
    public Long getIsTimestamp() {
        return isTimestamp;
    }

    @JsonProperty("is_timestamp")
    public void setIsTimestamp(Long isTimestamp) {
        this.isTimestamp = isTimestamp;
    }

    public SortingRule withIsTimestamp(Long isTimestamp) {
        this.isTimestamp = isTimestamp;
        return this;
    }

    @JsonProperty("is_object_name")
    public Long getIsObjectName() {
        return isObjectName;
    }

    @JsonProperty("is_object_name")
    public void setIsObjectName(Long isObjectName) {
        this.isObjectName = isObjectName;
    }

    public SortingRule withIsObjectName(Long isObjectName) {
        this.isObjectName = isObjectName;
        return this;
    }

    @JsonProperty("key_name")
    public String getKeyName() {
        return keyName;
    }

    @JsonProperty("key_name")
    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    public SortingRule withKeyName(String keyName) {
        this.keyName = keyName;
        return this;
    }

    @JsonProperty("descending")
    public Long getDescending() {
        return descending;
    }

    @JsonProperty("descending")
    public void setDescending(Long descending) {
        this.descending = descending;
    }

    public SortingRule withDescending(Long descending) {
        this.descending = descending;
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
        return ((((((((((("SortingRule"+" [isTimestamp=")+ isTimestamp)+", isObjectName=")+ isObjectName)+", keyName=")+ keyName)+", descending=")+ descending)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
