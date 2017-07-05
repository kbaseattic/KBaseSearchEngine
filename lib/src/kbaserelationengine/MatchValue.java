
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
 * <p>Original spec-file type: MatchValue</p>
 * <pre>
 * Optional rules of defining constraints for values of particular
 * term (keyword). Appropriate field depends on type of keyword.
 * For instance in case of integer type 'int_value' should be used.
 * In case of range constraint rather than single value 'min_*' 
 * and 'max_*' fields should be used. You may omit one of ends of
 * range to achieve '<=' or '>=' comparison. Ends are always
 * included for range constrains.
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "value",
    "int_value",
    "double_value",
    "bool_value",
    "min_int",
    "max_int",
    "min_date",
    "max_date",
    "min_double",
    "max_double"
})
public class MatchValue {

    @JsonProperty("value")
    private String value;
    @JsonProperty("int_value")
    private Long intValue;
    @JsonProperty("double_value")
    private Double doubleValue;
    @JsonProperty("bool_value")
    private Long boolValue;
    @JsonProperty("min_int")
    private Long minInt;
    @JsonProperty("max_int")
    private Long maxInt;
    @JsonProperty("min_date")
    private Long minDate;
    @JsonProperty("max_date")
    private Long maxDate;
    @JsonProperty("min_double")
    private Double minDouble;
    @JsonProperty("max_double")
    private Double maxDouble;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("value")
    public String getValue() {
        return value;
    }

    @JsonProperty("value")
    public void setValue(String value) {
        this.value = value;
    }

    public MatchValue withValue(String value) {
        this.value = value;
        return this;
    }

    @JsonProperty("int_value")
    public Long getIntValue() {
        return intValue;
    }

    @JsonProperty("int_value")
    public void setIntValue(Long intValue) {
        this.intValue = intValue;
    }

    public MatchValue withIntValue(Long intValue) {
        this.intValue = intValue;
        return this;
    }

    @JsonProperty("double_value")
    public Double getDoubleValue() {
        return doubleValue;
    }

    @JsonProperty("double_value")
    public void setDoubleValue(Double doubleValue) {
        this.doubleValue = doubleValue;
    }

    public MatchValue withDoubleValue(Double doubleValue) {
        this.doubleValue = doubleValue;
        return this;
    }

    @JsonProperty("bool_value")
    public Long getBoolValue() {
        return boolValue;
    }

    @JsonProperty("bool_value")
    public void setBoolValue(Long boolValue) {
        this.boolValue = boolValue;
    }

    public MatchValue withBoolValue(Long boolValue) {
        this.boolValue = boolValue;
        return this;
    }

    @JsonProperty("min_int")
    public Long getMinInt() {
        return minInt;
    }

    @JsonProperty("min_int")
    public void setMinInt(Long minInt) {
        this.minInt = minInt;
    }

    public MatchValue withMinInt(Long minInt) {
        this.minInt = minInt;
        return this;
    }

    @JsonProperty("max_int")
    public Long getMaxInt() {
        return maxInt;
    }

    @JsonProperty("max_int")
    public void setMaxInt(Long maxInt) {
        this.maxInt = maxInt;
    }

    public MatchValue withMaxInt(Long maxInt) {
        this.maxInt = maxInt;
        return this;
    }

    @JsonProperty("min_date")
    public Long getMinDate() {
        return minDate;
    }

    @JsonProperty("min_date")
    public void setMinDate(Long minDate) {
        this.minDate = minDate;
    }

    public MatchValue withMinDate(Long minDate) {
        this.minDate = minDate;
        return this;
    }

    @JsonProperty("max_date")
    public Long getMaxDate() {
        return maxDate;
    }

    @JsonProperty("max_date")
    public void setMaxDate(Long maxDate) {
        this.maxDate = maxDate;
    }

    public MatchValue withMaxDate(Long maxDate) {
        this.maxDate = maxDate;
        return this;
    }

    @JsonProperty("min_double")
    public Double getMinDouble() {
        return minDouble;
    }

    @JsonProperty("min_double")
    public void setMinDouble(Double minDouble) {
        this.minDouble = minDouble;
    }

    public MatchValue withMinDouble(Double minDouble) {
        this.minDouble = minDouble;
        return this;
    }

    @JsonProperty("max_double")
    public Double getMaxDouble() {
        return maxDouble;
    }

    @JsonProperty("max_double")
    public void setMaxDouble(Double maxDouble) {
        this.maxDouble = maxDouble;
    }

    public MatchValue withMaxDouble(Double maxDouble) {
        this.maxDouble = maxDouble;
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
        return ((((((((((((((((((((((("MatchValue"+" [value=")+ value)+", intValue=")+ intValue)+", doubleValue=")+ doubleValue)+", boolValue=")+ boolValue)+", minInt=")+ minInt)+", maxInt=")+ maxInt)+", minDate=")+ minDate)+", maxDate=")+ maxDate)+", minDouble=")+ minDouble)+", maxDouble=")+ maxDouble)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
