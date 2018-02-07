
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
import us.kbase.common.service.Tuple5;


/**
 * <p>Original spec-file type: SearchObjectsOutput</p>
 * <pre>
 * Output results for 'search_objects' method.
 * 'pagination' and 'sorting_rules' fields show actual input for
 *     pagination and sorting.
 * total - total number of found objects.
 * search_time - common time in milliseconds spent.
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "pagination",
    "sorting_rules",
    "objects",
    "total",
    "search_time",
    "access_group_narrative_info"
})
public class SearchObjectsOutput {

    /**
     * <p>Original spec-file type: Pagination</p>
     * <pre>
     * Pagination rules. Default values are: start = 0, count = 50.
     * </pre>
     * 
     */
    @JsonProperty("pagination")
    private Pagination pagination;
    @JsonProperty("sorting_rules")
    private List<SortingRule> sortingRules;
    @JsonProperty("objects")
    private List<ObjectData> objects;
    @JsonProperty("total")
    private java.lang.Long total;
    @JsonProperty("search_time")
    private java.lang.Long searchTime;
    @JsonProperty("access_group_narrative_info")
    private Map<Long, Tuple5 <String, Long, Long, String, String>> accessGroupNarrativeInfo;
    private Map<java.lang.String, Object> additionalProperties = new HashMap<java.lang.String, Object>();

    /**
     * <p>Original spec-file type: Pagination</p>
     * <pre>
     * Pagination rules. Default values are: start = 0, count = 50.
     * </pre>
     * 
     */
    @JsonProperty("pagination")
    public Pagination getPagination() {
        return pagination;
    }

    /**
     * <p>Original spec-file type: Pagination</p>
     * <pre>
     * Pagination rules. Default values are: start = 0, count = 50.
     * </pre>
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
    public java.lang.Long getTotal() {
        return total;
    }

    @JsonProperty("total")
    public void setTotal(java.lang.Long total) {
        this.total = total;
    }

    public SearchObjectsOutput withTotal(java.lang.Long total) {
        this.total = total;
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

    public SearchObjectsOutput withSearchTime(java.lang.Long searchTime) {
        this.searchTime = searchTime;
        return this;
    }

    @JsonProperty("access_group_narrative_info")
    public Map<Long, Tuple5 <String, Long, Long, String, String>> getAccessGroupNarrativeInfo() {
        return accessGroupNarrativeInfo;
    }

    @JsonProperty("access_group_narrative_info")
    public void setAccessGroupNarrativeInfo(Map<Long, Tuple5 <String, Long, Long, String, String>> accessGroupNarrativeInfo) {
        this.accessGroupNarrativeInfo = accessGroupNarrativeInfo;
    }

    public SearchObjectsOutput withAccessGroupNarrativeInfo(Map<Long, Tuple5 <String, Long, Long, String, String>> accessGroupNarrativeInfo) {
        this.accessGroupNarrativeInfo = accessGroupNarrativeInfo;
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
        return ((((((((((((((("SearchObjectsOutput"+" [pagination=")+ pagination)+", sortingRules=")+ sortingRules)+", objects=")+ objects)+", total=")+ total)+", searchTime=")+ searchTime)+", accessGroupNarrativeInfo=")+ accessGroupNarrativeInfo)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
