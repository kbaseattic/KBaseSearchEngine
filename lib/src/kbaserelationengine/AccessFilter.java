
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
 * <p>Original spec-file type: AccessFilter</p>
 * <pre>
 * Optional rules of access constrains.
 *   - with_private - include data found in workspaces not marked 
 *       as public, default value is true,
 *   - with_public - include data found in public workspaces,
 *       default value is false,
 *   - with_all_history - include all versions (last one and all
 *       old versions) of objects matching constrains, default
 *       value is false.
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "with_private",
    "with_public",
    "with_all_history"
})
public class AccessFilter {

    @JsonProperty("with_private")
    private Long withPrivate;
    @JsonProperty("with_public")
    private Long withPublic;
    @JsonProperty("with_all_history")
    private Long withAllHistory;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("with_private")
    public Long getWithPrivate() {
        return withPrivate;
    }

    @JsonProperty("with_private")
    public void setWithPrivate(Long withPrivate) {
        this.withPrivate = withPrivate;
    }

    public AccessFilter withWithPrivate(Long withPrivate) {
        this.withPrivate = withPrivate;
        return this;
    }

    @JsonProperty("with_public")
    public Long getWithPublic() {
        return withPublic;
    }

    @JsonProperty("with_public")
    public void setWithPublic(Long withPublic) {
        this.withPublic = withPublic;
    }

    public AccessFilter withWithPublic(Long withPublic) {
        this.withPublic = withPublic;
        return this;
    }

    @JsonProperty("with_all_history")
    public Long getWithAllHistory() {
        return withAllHistory;
    }

    @JsonProperty("with_all_history")
    public void setWithAllHistory(Long withAllHistory) {
        this.withAllHistory = withAllHistory;
    }

    public AccessFilter withWithAllHistory(Long withAllHistory) {
        this.withAllHistory = withAllHistory;
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
        return ((((((((("AccessFilter"+" [withPrivate=")+ withPrivate)+", withPublic=")+ withPublic)+", withAllHistory=")+ withAllHistory)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
