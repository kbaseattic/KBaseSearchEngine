
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
import us.kbase.common.service.Tuple11;
import us.kbase.common.service.Tuple5;
import us.kbase.common.service.Tuple9;


/**
 * <p>Original spec-file type: GetObjectsOutput</p>
 * <pre>
 * Output results of get_objects method.
 * list<GUID> removedGuids - list of result GUIDs that are inaccessible or deleted in the workspace but have
 *     not been updated in search
 * mapping<access_group_id, narrative_info> access_group_narrative_info - information about
 *    the workspaces in which the objects in the results reside. This data only applies to
 *    workspace objects.
 * mapping<access_group_id, access_group_info> access_groups_info - information about
 *    the access groups in which the objects in the results reside. Currently this data only applies to
 *    workspace objects. The tuple9 value returned by get_workspace_info() for each workspace
 *    in the search results is saved in this mapping. In future the access_group_info will be
 *    replaced with a higher level abstraction.
 * mapping<obj_ref, object_info> objects_info - information about each object in the
 *    search results. Currently this data only applies to workspace objects. The tuple11 value
 *    returned by get_object_info3() for each object in the search results is saved in the mapping.
 *    In future the object_info will be replaced with a higher level abstraction.
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "objects",
    "search_time",
    "removedGuids",
    "access_group_narrative_info",
    "access_groups_info",
    "objects_info"
})
public class GetObjectsOutput {

    @JsonProperty("objects")
    private List<ObjectData> objects;
    @JsonProperty("search_time")
    private java.lang.Long searchTime;
    @JsonProperty("removedGuids")
    private List<String> removedGuids;
    @JsonProperty("access_group_narrative_info")
    private Map<Long, Tuple5 <String, Long, Long, String, String>> accessGroupNarrativeInfo;
    @JsonProperty("access_groups_info")
    private Map<Long, Tuple9 <Long, String, String, String, Long, String, String, String, Map<String, String>>> accessGroupsInfo;
    @JsonProperty("objects_info")
    private Map<String, Tuple11 <Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> objectsInfo;
    private Map<java.lang.String, Object> additionalProperties = new HashMap<java.lang.String, Object>();

    @JsonProperty("objects")
    public List<ObjectData> getObjects() {
        return objects;
    }

    @JsonProperty("objects")
    public void setObjects(List<ObjectData> objects) {
        this.objects = objects;
    }

    public GetObjectsOutput withObjects(List<ObjectData> objects) {
        this.objects = objects;
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

    public GetObjectsOutput withSearchTime(java.lang.Long searchTime) {
        this.searchTime = searchTime;
        return this;
    }

    @JsonProperty("removedGuids")
    public List<String> getRemovedGuids() {
        return removedGuids;
    }

    @JsonProperty("removedGuids")
    public void setRemovedGuids(List<String> removedGuids) {
        this.removedGuids = removedGuids;
    }

    public GetObjectsOutput withRemovedGuids(List<String> removedGuids) {
        this.removedGuids = removedGuids;
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

    public GetObjectsOutput withAccessGroupNarrativeInfo(Map<Long, Tuple5 <String, Long, Long, String, String>> accessGroupNarrativeInfo) {
        this.accessGroupNarrativeInfo = accessGroupNarrativeInfo;
        return this;
    }

    @JsonProperty("access_groups_info")
    public Map<Long, Tuple9 <Long, String, String, String, Long, String, String, String, Map<String, String>>> getAccessGroupsInfo() {
        return accessGroupsInfo;
    }

    @JsonProperty("access_groups_info")
    public void setAccessGroupsInfo(Map<Long, Tuple9 <Long, String, String, String, Long, String, String, String, Map<String, String>>> accessGroupsInfo) {
        this.accessGroupsInfo = accessGroupsInfo;
    }

    public GetObjectsOutput withAccessGroupsInfo(Map<Long, Tuple9 <Long, String, String, String, Long, String, String, String, Map<String, String>>> accessGroupsInfo) {
        this.accessGroupsInfo = accessGroupsInfo;
        return this;
    }

    @JsonProperty("objects_info")
    public Map<String, Tuple11 <Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> getObjectsInfo() {
        return objectsInfo;
    }

    @JsonProperty("objects_info")
    public void setObjectsInfo(Map<String, Tuple11 <Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> objectsInfo) {
        this.objectsInfo = objectsInfo;
    }

    public GetObjectsOutput withObjectsInfo(Map<String, Tuple11 <Long, String, String, String, Long, String, Long, String, String, Long, Map<String, String>>> objectsInfo) {
        this.objectsInfo = objectsInfo;
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
        return ((((((((((((((("GetObjectsOutput"+" [objects=")+ objects)+", searchTime=")+ searchTime)+", removedGuids=")+ removedGuids)+", accessGroupNarrativeInfo=")+ accessGroupNarrativeInfo)+", accessGroupsInfo=")+ accessGroupsInfo)+", objectsInfo=")+ objectsInfo)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
