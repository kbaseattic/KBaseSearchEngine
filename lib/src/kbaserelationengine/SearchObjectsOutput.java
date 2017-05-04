
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
 * <p>Original spec-file type: SearchObjectsOutput</p>
 * 
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "pagination",
    "sorting_rules",
    "objects",
    "total",
    "search_time"
})
public class SearchObjectsOutput {

    /**
     * <p>Original spec-file type: Pagination</p>
     * 
     * 
     */
    @JsonProperty("pagination")
    private Pagination pagination;
    @JsonProperty("sorting_rules")
    private List<SortingRule> sortingRules;
    @JsonProperty("objects")
    private List<ObjectData> objects;
    @JsonProperty("total")
    private Long total;
    @JsonProperty("search_time")
    private Long searchTime;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    /**
     * <p>Original spec-file type: Pagination</p>
     * 
     * 
     */
    @JsonProperty("pagination")
    public Pagination getPagination() {
        return pagination;
    }

    /**
     * <p>Original spec-file type: Pagination</p>
     * 
     * 
     */
    @JsonProperty("pagination")
    public void setPagination(Pagination pagination) {
        this.pagination = pagination;
    }

    public SearchObjectsOutput withPagination(Pagination pagination) {
        this.pagination = pagination;
        return this;
    }

    @JsonProperty("sorting_rules")
    public List<SortingRule> getSortingRules() {
        return sortingRules;
    }

    @JsonProperty("sorting_rules")
    public void setSortingRules(List<SortingRule> sortingRules) {
        this.sortingRules = sortingRules;
    }

    public SearchObjectsOutput withSortingRules(List<SortingRule> sortingRules) {
        this.sortingRules = sortingRules;
        return this;
    }

    @JsonProperty("objects")
    public List<ObjectData> getObjects() {
        return objects;
    }

    @JsonProperty("objects")
    public void setObjects(List<ObjectData> objects) {
        this.objects = objects;
    }

    public SearchObjectsOutput withObjects(List<ObjectData> objects) {
        this.objects = objects;
        return this;
    }

    @JsonProperty("total")
    public Long getTotal() {
        return total;
    }

    @JsonProperty("total")
    public void setTotal(Long total) {
        this.total = total;
    }

    public SearchObjectsOutput withTotal(Long total) {
        this.total = total;
        return this;
    }

    @JsonProperty("search_time")
    public Long getSearchTime() {
        return searchTime;
    }

    @JsonProperty("search_time")
    public void setSearchTime(Long searchTime) {
        this.searchTime = searchTime;
    }

    public SearchObjectsOutput withSearchTime(Long searchTime) {
        this.searchTime = searchTime;
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
        return ((((((((((((("SearchObjectsOutput"+" [pagination=")+ pagination)+", sortingRules=")+ sortingRules)+", objects=")+ objects)+", total=")+ total)+", searchTime=")+ searchTime)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
