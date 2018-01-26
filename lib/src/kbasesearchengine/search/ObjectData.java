package kbasesearchengine.search;

import java.lang.reflect.Array;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Optional;

import kbasesearchengine.common.GUID;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.tools.Utils;

/** Data about an object in the search system. Contains a minimum of the object GUID, and may
 * contain more fields depending on the specification of which fields to include.
 * @author gaprice@lbl.gov
 *
 */
public class ObjectData {
    
    private final GUID guid;
    private final Optional<GUID> parentGuid;
    private final Optional<String> objectName;
    private final Optional<SearchObjectType> type;
    private final Optional<String> creator;
    private final Optional<String> copier;
    private final Optional<String> module;
    private final Optional<String> method;
    private final Optional<String> commitHash;
    private final Optional<String> moduleVersion;
    private final Optional<String> md5;
    private final Optional<Instant> timestamp;
    private final Optional<Object> parentData;
    private final Optional<Object> data;
    private final Map<String, String> keyProps;
    private final Map<String, ArrayList> highlight;


    private ObjectData(
            final GUID guid,
            final String objectName,
            final SearchObjectType type,
            final String creator,
            final String copier,
            final String module,
            final String method,
            final String commitHash,
            final String moduleVersion,
            final String md5,
            final Instant timestamp,
            final Object parentData,
            final Object data,
            final Map<String, String> keyProps,
            final Map<String, ArrayList> highlight) {
        this.guid = guid;
        if (parentData != null) {
            this.parentGuid = Optional.fromNullable(new GUID(guid, null, null));
        } else {
            this.parentGuid = Optional.absent();
        }
        this.objectName = Optional.fromNullable(objectName);
        this.type = Optional.fromNullable(type);
        this.creator = Optional.fromNullable(creator);
        this.copier = Optional.fromNullable(copier);
        this.module = Optional.fromNullable(module);
        this.method = Optional.fromNullable(method);
        this.commitHash = Optional.fromNullable(commitHash);
        this.moduleVersion = Optional.fromNullable(moduleVersion);
        this.md5 = Optional.fromNullable(md5);
        this.timestamp = Optional.fromNullable(timestamp);
        this.parentData = Optional.fromNullable(parentData);
        this.data = Optional.fromNullable(data);
        this.keyProps = Collections.unmodifiableMap(keyProps);
        this.highlight = Collections.unmodifiableMap(highlight);
    }

    /** Get the object's GUID.
     * @return the GUID.
     */

    public GUID getGUID() {
        return guid;
    }

    /** Get the parent object's GUID (e.g. the object guid without the subobject type or id).
     * Only present if the parent object's data is present (see {@link #getParentData()}).
     * @return the parent GUID.
     */
    public Optional<GUID> getParentGUID() {
        return parentGuid;
    }

    /** Get the name of the object, if present.
     * @return the object name.
     */
    public Optional<String> getObjectName() {
        return objectName;
    }

    /** Get the type of the object, if present. 
     * @return the object type.
     */
    public Optional<SearchObjectType> getType() {
        return type;
    }

    /** Get the username of the creator of the object, if present.
     * @return the object's creator.
     */
    public Optional<String> getCreator() {
        return creator;
    }

    /** Get the username of copier of the object, if present.
     * @return the user that copied the object.
     */
    public Optional<String> getCopier() {
        return copier;
    }

    /** Get the name of the software module used to create the object, if present.
     * @return the module name.
     */
    public Optional<String> getModule() {
        return module;
    }

    /** Get the name of the software method used to create the object, if present.
     * @return the method name.
     */
    public Optional<String> getMethod() {
        return method;
    }

    /** Get the commit hash of the software used to create the object, if present.
     * @return the commit hash.
     */
    public Optional<String> getCommitHash() {
        return commitHash;
    }

    /** Get the version of the software module used to create the object, if present.
     * @return the module version.
     */
    public Optional<String> getModuleVersion() {
        return moduleVersion;
    }

    /** Get the md5 digest of the object, if present.
     * @return the md5.
     */
    public Optional<String> getMd5() {
        return md5;
    }

    /** Get the timestamp of the creation of the object, if present.
     * @return the timestamp.
     */
    public Optional<Instant> getTimestamp() {
        return timestamp;
    }

    /** Get the data associated with the parent of the object, if present. If present, the 
     * parent GUID will also be available (see {@link #getParentGUID()}).
     * @return the parent object data.
     */
    public Optional<Object> getParentData() {
        return parentData;
    }

    /** Get the data associated with the object, if present.
     * @return the object data.
     */
    public Optional<Object> getData() {
        return data;
    }

    /** Get the properties extracted from the object data and stored as searchable keys in the
     * search system.
     * @return the key properties.
     */
    public Map<String, String> getKeyProperties() {
        return keyProps;
    }

    /** Get hits that matched the query as highlighted snips corresponding to fields.
     * @return the all fields with highlighting matches.
     */
    public Map<String, ArrayList> getHighlight() { return highlight; }



    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((commitHash == null) ? 0 : commitHash.hashCode());
        result = prime * result + ((copier == null) ? 0 : copier.hashCode());
        result = prime * result + ((creator == null) ? 0 : creator.hashCode());
        result = prime * result + ((data == null) ? 0 : data.hashCode());
        result = prime * result + ((guid == null) ? 0 : guid.hashCode());
        result = prime * result
                + ((keyProps == null) ? 0 : keyProps.hashCode());
        result = prime * result + ((md5 == null) ? 0 : md5.hashCode());
        result = prime * result + ((method == null) ? 0 : method.hashCode());
        result = prime * result + ((module == null) ? 0 : module.hashCode());
        result = prime * result
                + ((moduleVersion == null) ? 0 : moduleVersion.hashCode());
        result = prime * result
                + ((objectName == null) ? 0 : objectName.hashCode());
        result = prime * result
                + ((parentData == null) ? 0 : parentData.hashCode());
        result = prime * result
                + ((parentGuid == null) ? 0 : parentGuid.hashCode());
        result = prime * result
                + ((timestamp == null) ? 0 : timestamp.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + ((highlight == null) ? 0 : highlight.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ObjectData other = (ObjectData) obj;
        if (commitHash == null) {
            if (other.commitHash != null) {
                return false;
            }
        } else if (!commitHash.equals(other.commitHash)) {
            return false;
        }
        if (copier == null) {
            if (other.copier != null) {
                return false;
            }
        } else if (!copier.equals(other.copier)) {
            return false;
        }
        if (creator == null) {
            if (other.creator != null) {
                return false;
            }
        } else if (!creator.equals(other.creator)) {
            return false;
        }
        if (data == null) {
            if (other.data != null) {
                return false;
            }
        } else if (!data.equals(other.data)) {
            return false;
        }
        if (guid == null) {
            if (other.guid != null) {
                return false;
            }
        } else if (!guid.equals(other.guid)) {
            return false;
        }
        if (keyProps == null) {
            if (other.keyProps != null) {
                return false;
            }
        } else if (!keyProps.equals(other.keyProps)) {
            return false;
        }
        if (md5 == null) {
            if (other.md5 != null) {
                return false;
            }
        } else if (!md5.equals(other.md5)) {
            return false;
        }
        if (method == null) {
            if (other.method != null) {
                return false;
            }
        } else if (!method.equals(other.method)) {
            return false;
        }
        if (module == null) {
            if (other.module != null) {
                return false;
            }
        } else if (!module.equals(other.module)) {
            return false;
        }
        if (moduleVersion == null) {
            if (other.moduleVersion != null) {
                return false;
            }
        } else if (!moduleVersion.equals(other.moduleVersion)) {
            return false;
        }
        if (objectName == null) {
            if (other.objectName != null) {
                return false;
            }
        } else if (!objectName.equals(other.objectName)) {
            return false;
        }
        if (parentData == null) {
            if (other.parentData != null) {
                return false;
            }
        } else if (!parentData.equals(other.parentData)) {
            return false;
        }
        if (parentGuid == null) {
            if (other.parentGuid != null) {
                return false;
            }
        } else if (!parentGuid.equals(other.parentGuid)) {
            return false;
        }
        if (timestamp == null) {
            if (other.timestamp != null) {
                return false;
            }
        } else if (!timestamp.equals(other.timestamp)) {
            return false;
        }
        if (type == null) {
            if (other.type != null) {
                return false;
            }
        } else if (!type.equals(other.type)) {
            return false;
        }
        if (highlight == null) {
            if (other.highlight != null) {
                return false;
            }
        } else if (!highlight.equals(other.highlight)) {
            return false;
        }
        return true;
    }
    
    /** Get a builder for an {@link ObjectData} instance.
     * @param guid the GUID fro the object.
     * @return a new builder.
     */
    public static Builder getBuilder(final GUID guid) {
        return new Builder(guid);
    }
    
    /** An {@link ObjectData} builder.
     * @author gaprice@lbl.gov
     *
     */
    public static class Builder {
        
        private final GUID guid;
        private String objectName;
        private SearchObjectType type;
        private String creator;
        private String copier;
        private String module;
        private String method;
        private String commitHash;
        private String moduleVersion;
        private String md5;
        private Instant timestamp;
        private Object parentData;
        private Object data;
        private Map<String, String> keyProps = new HashMap<>();
        private Map<String, ArrayList> highlight = new HashMap<>();
        
        private Builder(final GUID guid) {
            Utils.nonNull(guid, "guid");
            this.guid = guid;
        }
        
        /** Build the new ObjectData.
         * @return the object data.
         */
        public ObjectData build() {
            return new ObjectData(guid, objectName, type, creator, copier, module, method,
                    commitHash, moduleVersion, md5, timestamp, parentData, data, keyProps, highlight);
        }
        
        /** Set the object name in the builder. Replaces any previous object name. Nulls and
         * whitespace only names are ignored.
         * @param objectName the object name.
         * @return this builder.
         */
        public Builder withNullableObjectName(final String objectName) {
            if (!Utils.isNullOrEmpty(objectName)) {
                this.objectName = objectName;
            }
            return this;
        }
        
        /** Set the object type in the builder. Replaces any previous type. Null is ignored.
         * @param type the object type.
         * @return this builder.
         */
        public Builder withNullableType(final SearchObjectType type) {
            if (type != null) {
                this.type = type;
            }
            return this;
        }
        
        /** Set the creator's user name in the builder. Replaces any previous creator. Nulls and
         * whitespace only names are ignored.
         * @param creator the creator's user name.
         * @return this builder.
         */
        public Builder withNullableCreator(final String creator) {
            if (!Utils.isNullOrEmpty(creator)) {
                this.creator = creator;
            }
            return this;
        }
        
        /** Set the user name of the user that copied the object in the builder.
         * Replaces any previous copier name. Nulls and whitespace only names are ignored.
         * @param copier the copier's user name.
         * @return this builder.
         */
        public Builder withNullableCopier(final String copier) {
            if (!Utils.isNullOrEmpty(copier)) {
                this.copier = copier;
            }
            return this;
        }
        
        /** Set the name of the software module used to create the object in the builder.
         * Replaces any previous module name. Nulls and whitespace only names are ignored.
         * @param module the module name.
         * @return this builder.
         */
        public Builder withNullableModule(final String module) {
            if (!Utils.isNullOrEmpty(module)) {
                this.module = module;
            }
            return this;
        }
        
        /** Set the name of the software method used to create the object in the builder.
         * Replaces any previous object name. Nulls and whitespace only names are ignored.
         * @param method the method name.
         * @return this builder.
         */
        public Builder withNullableMethod(final String method) {
            if (!Utils.isNullOrEmpty(method)) {
                this.method = method;
            }
            return this;
        }
        
        /** Set the commit hash of the software module used to create the object in the builder.
         * Replaces any previous hash. Nulls and whitespace only hashes are ignored.
         * @param commitHash the commit hash.
         * @return this builder.
         */
        public Builder withNullableCommitHash(final String commitHash) {
            if (!Utils.isNullOrEmpty(commitHash)) {
                this.commitHash = commitHash;
            }
            return this;
        }
        
        /** Set the version of the software module used to create the object in the builder.
         * Replaces any previous version. Nulls and whitespace only versions are ignored.
         * @param moduleVersion the module version.
         * @return this builder.
         */
        public Builder withNullableModuleVersion(final String moduleVersion) {
            if (!Utils.isNullOrEmpty(moduleVersion)) {
                this.moduleVersion = moduleVersion;
            }
            return this;
        }
        
        /** Set the MD5 digest of the object.
         * Replaces any previous MD5s. Nulls and whitespace only MD5s are ignored.
         * @param md5 the MD5.
         * @return this builder.
         */
        public Builder withNullableMD5(final String md5) {
            if (!Utils.isNullOrEmpty(md5)) {
                this.md5 = md5;
            }
            return this;
        }
        
        /** Set the object's creation timestamp. in the builder. Replaces any previous timestamp.
         * Null is ignored.
         * @param timestamp the timestamp.
         * @return this builder.
         */
        public Builder withNullableTimestamp(final Instant timestamp) {
            if (timestamp != null) {
                this.timestamp = timestamp;
            }
            return this;
        }
        
        /** Set the data of the parent object in the builder. Replaces any previous parent data.
         * Null is ignored. If the parent data is present, the parent GUID
         * (see {@link ObjectData#getParentGUID()}) will be available.
         * @param parentData the parent object's data.
         * @return this builder.
         */
        public Builder withNullableParentData(final Object parentData) {
            if (parentData != null) {
                this.parentData = parentData;
            }
            return this;
        }
        
        /** Set the data of the object in the builder. Replaces any previous data.
         * Null is ignored.
         * @param data the object's data.
         * @return this builder.
         */
        public Builder withNullableData(final Object data) {
            if (data != null) {
                this.data = data;
            }
            return this;
        }
        
        /** Adds a searchable key and value to the builder. Keys must be non-null and consist of at
         * least one non-whitespace character. Adding a duplicate key will overwrite the value of
         * the previous key.
         * @param key the key.
         * @param property the value.
         * @return this builder.
         */
        public Builder withKeyProperty(final String key, final String property) {
            Utils.notNullOrEmpty(key, "key cannot be null or whitespace");
            keyProps.put(key, property);
            return this;
        }

        /** Adds the highlight fields to the object.
         * @param highlight the map of fields returned from elasticsearch.
         * @return this builder.
         */
        public Builder withHighlight(final String field, final ArrayList highlight) {
            Utils.notNullOrEmpty(field, "field cannot be null or whitespace");
            Utils.noNulls(highlight, "highlight cannot be null");
            if(highlight.size() > 0) {
                this.highlight.put(field, highlight);
            }
            return this;
        }
    }
}
