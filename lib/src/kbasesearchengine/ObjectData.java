
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
import us.kbase.common.service.UObject;


/**
 * <p>Original spec-file type: ObjectData</p>
 * <pre>
 * Properties of an object including metadata, raw data and keywords.
 * GUID guid - the object's guid.
 * GUID parent_guid - the guid of the object's parent if the object is a subobject (e.g.
 *     features for genomes).
 * object_name - the object's name.
 * timestamp - the creation date for the object in milliseconds since the epoch.
 * string type - the type of the data in the search index.
 * int type_ver - the version of the search type.
 * string creator - the username of the user that created that data.
 * string copier - if this instance of the data is a copy, the username of the user that
 *     copied the data.
 * string mod - the name of the KBase SDK module that was used to create the data.
 * string method - the name of the method in the KBase SDK module that was used to create the
 *     data.
 * string module_ver - the version of the KBase SDK module that was used to create the data.
 * string commit - the version control commit hash of the KBase SDK module that was used to
 *     create the data. 
 * parent_data - raw data extracted from the subobject's parent object. The data contents will
 *     vary from object to object. Null if the object is not a subobject.
 * data - raw data extracted from the object. The data contents will vary from object to object.
 * key_props - keyword properties of the object. These fields have been extracted from the object
 *    and possibly transformed based on the search specification for the object.
 *    The contents will vary from object to object.
 * mapping<string, list<string>> highlight - The keys are the field names and the list 
 *     contains the sections in each field that matched the search query. Fields with no
 *     hits will not be available. Short fields that matched are shown in their entirety.
 *     Longer fields are shown as snippets preceded or followed by "...".
 * </pre>
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "guid",
    "parent_guid",
    "object_name",
    "timestamp",
    "type",
    "type_ver",
    "creator",
    "copier",
    "mod",
    "method",
    "module_ver",
    "commit",
    "parent_data",
    "data",
    "key_props",
    "highlight"
})
public class ObjectData {

    @JsonProperty("guid")
    private java.lang.String guid;
    @JsonProperty("parent_guid")
    private java.lang.String parentGuid;
    @JsonProperty("object_name")
    private java.lang.String objectName;
    @JsonProperty("timestamp")
    private Long timestamp;
    @JsonProperty("type")
    private java.lang.String type;
    @JsonProperty("type_ver")
    private Long typeVer;
    @JsonProperty("creator")
    private java.lang.String creator;
    @JsonProperty("copier")
    private java.lang.String copier;
    @JsonProperty("mod")
    private java.lang.String mod;
    @JsonProperty("method")
    private java.lang.String method;
    @JsonProperty("module_ver")
    private java.lang.String moduleVer;
    @JsonProperty("commit")
    private java.lang.String commit;
    @JsonProperty("parent_data")
    private UObject parentData;
    @JsonProperty("data")
    private UObject data;
    @JsonProperty("key_props")
    private Map<String, String> keyProps;
    @JsonProperty("highlight")
    private Map<String, List<String>> highlight;
    private Map<java.lang.String, Object> additionalProperties = new HashMap<java.lang.String, Object>();

    @JsonProperty("guid")
    public java.lang.String getGuid() {
        return guid;
    }

    @JsonProperty("guid")
    public void setGuid(java.lang.String guid) {
        this.guid = guid;
    }

    public ObjectData withGuid(java.lang.String guid) {
        this.guid = guid;
        return this;
    }

    @JsonProperty("parent_guid")
    public java.lang.String getParentGuid() {
        return parentGuid;
    }

    @JsonProperty("parent_guid")
    public void setParentGuid(java.lang.String parentGuid) {
        this.parentGuid = parentGuid;
    }

    public ObjectData withParentGuid(java.lang.String parentGuid) {
        this.parentGuid = parentGuid;
        return this;
    }

    @JsonProperty("object_name")
    public java.lang.String getObjectName() {
        return objectName;
    }

    @JsonProperty("object_name")
    public void setObjectName(java.lang.String objectName) {
        this.objectName = objectName;
    }

    public ObjectData withObjectName(java.lang.String objectName) {
        this.objectName = objectName;
        return this;
    }

    @JsonProperty("timestamp")
    public Long getTimestamp() {
        return timestamp;
    }

    @JsonProperty("timestamp")
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public ObjectData withTimestamp(Long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    @JsonProperty("type")
    public java.lang.String getType() {
        return type;
    }

    @JsonProperty("type")
    public void setType(java.lang.String type) {
        this.type = type;
    }

    public ObjectData withType(java.lang.String type) {
        this.type = type;
        return this;
    }

    @JsonProperty("type_ver")
    public Long getTypeVer() {
        return typeVer;
    }

    @JsonProperty("type_ver")
    public void setTypeVer(Long typeVer) {
        this.typeVer = typeVer;
    }

    public ObjectData withTypeVer(Long typeVer) {
        this.typeVer = typeVer;
        return this;
    }

    @JsonProperty("creator")
    public java.lang.String getCreator() {
        return creator;
    }

    @JsonProperty("creator")
    public void setCreator(java.lang.String creator) {
        this.creator = creator;
    }

    public ObjectData withCreator(java.lang.String creator) {
        this.creator = creator;
        return this;
    }

    @JsonProperty("copier")
    public java.lang.String getCopier() {
        return copier;
    }

    @JsonProperty("copier")
    public void setCopier(java.lang.String copier) {
        this.copier = copier;
    }

    public ObjectData withCopier(java.lang.String copier) {
        this.copier = copier;
        return this;
    }

    @JsonProperty("mod")
    public java.lang.String getMod() {
        return mod;
    }

    @JsonProperty("mod")
    public void setMod(java.lang.String mod) {
        this.mod = mod;
    }

    public ObjectData withMod(java.lang.String mod) {
        this.mod = mod;
        return this;
    }

    @JsonProperty("method")
    public java.lang.String getMethod() {
        return method;
    }

    @JsonProperty("method")
    public void setMethod(java.lang.String method) {
        this.method = method;
    }

    public ObjectData withMethod(java.lang.String method) {
        this.method = method;
        return this;
    }

    @JsonProperty("module_ver")
    public java.lang.String getModuleVer() {
        return moduleVer;
    }

    @JsonProperty("module_ver")
    public void setModuleVer(java.lang.String moduleVer) {
        this.moduleVer = moduleVer;
    }

    public ObjectData withModuleVer(java.lang.String moduleVer) {
        this.moduleVer = moduleVer;
        return this;
    }

    @JsonProperty("commit")
    public java.lang.String getCommit() {
        return commit;
    }

    @JsonProperty("commit")
    public void setCommit(java.lang.String commit) {
        this.commit = commit;
    }

    public ObjectData withCommit(java.lang.String commit) {
        this.commit = commit;
        return this;
    }

    @JsonProperty("parent_data")
    public UObject getParentData() {
        return parentData;
    }

    @JsonProperty("parent_data")
    public void setParentData(UObject parentData) {
        this.parentData = parentData;
    }

    public ObjectData withParentData(UObject parentData) {
        this.parentData = parentData;
        return this;
    }

    @JsonProperty("data")
    public UObject getData() {
        return data;
    }

    @JsonProperty("data")
    public void setData(UObject data) {
        this.data = data;
    }

    public ObjectData withData(UObject data) {
        this.data = data;
        return this;
    }

    @JsonProperty("key_props")
    public Map<String, String> getKeyProps() {
        return keyProps;
    }

    @JsonProperty("key_props")
    public void setKeyProps(Map<String, String> keyProps) {
        this.keyProps = keyProps;
    }

    public ObjectData withKeyProps(Map<String, String> keyProps) {
        this.keyProps = keyProps;
        return this;
    }

    @JsonProperty("highlight")
    public Map<String, List<String>> getHighlight() {
        return highlight;
    }

    @JsonProperty("highlight")
    public void setHighlight(Map<String, List<String>> highlight) {
        this.highlight = highlight;
    }

    public ObjectData withHighlight(Map<String, List<String>> highlight) {
        this.highlight = highlight;
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
        return ((((((((((((((((((((((((((((((((((("ObjectData"+" [guid=")+ guid)+", parentGuid=")+ parentGuid)+", objectName=")+ objectName)+", timestamp=")+ timestamp)+", type=")+ type)+", typeVer=")+ typeVer)+", creator=")+ creator)+", copier=")+ copier)+", mod=")+ mod)+", method=")+ method)+", moduleVer=")+ moduleVer)+", commit=")+ commit)+", parentData=")+ parentData)+", data=")+ data)+", keyProps=")+ keyProps)+", highlight=")+ highlight)+", additionalProperties=")+ additionalProperties)+"]");
    }

}
