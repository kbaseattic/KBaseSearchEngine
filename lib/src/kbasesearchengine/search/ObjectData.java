package kbasesearchengine.search;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Optional;

import kbasesearchengine.common.GUID;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.tools.Utils;

public class ObjectData {
    
    //TODO JAVADOC
    //TODO TESTS
    
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
            final Map<String, String> keyProps) {
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
    }

    public GUID getGUID() {
        return guid;
    }

    public Optional<GUID> getParentGUID() {
        return parentGuid;
    }

    public Optional<String> getObjectName() {
        return objectName;
    }

    public Optional<SearchObjectType> getType() {
        return type;
    }

    public Optional<String> getCreator() {
        return creator;
    }

    public Optional<String> getCopier() {
        return copier;
    }

    public Optional<String> getModule() {
        return module;
    }

    public Optional<String> getMethod() {
        return method;
    }

    public Optional<String> getCommitHash() {
        return commitHash;
    }

    public Optional<String> getModuleVersion() {
        return moduleVersion;
    }

    public Optional<String> getMd5() {
        return md5;
    }

    public Optional<Instant> getTimestamp() {
        return timestamp;
    }

    public Optional<Object> getParentData() {
        return parentData;
    }

    public Optional<Object> getData() {
        return data;
    }

    //TODO NNOW change to getKeyProperties, add getKeyProperty(key)
    
    public Map<String, String> getKeyProps() {
        return keyProps;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ObjectData [guid=");
        builder.append(guid);
        builder.append(", parentGuid=");
        builder.append(parentGuid);
        builder.append(", objectName=");
        builder.append(objectName);
        builder.append(", type=");
        builder.append(type);
        builder.append(", creator=");
        builder.append(creator);
        builder.append(", copier=");
        builder.append(copier);
        builder.append(", module=");
        builder.append(module);
        builder.append(", method=");
        builder.append(method);
        builder.append(", commitHash=");
        builder.append(commitHash);
        builder.append(", moduleVersion=");
        builder.append(moduleVersion);
        builder.append(", md5=");
        builder.append(md5);
        builder.append(", timestamp=");
        builder.append(timestamp);
        builder.append(", parentData=");
        builder.append(parentData);
        builder.append(", data=");
        builder.append(data);
        builder.append(", keyProps=");
        builder.append(keyProps);
        builder.append("]");
        return builder.toString();
    }

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
        return true;
    }
    
    public static Builder getBuilder(final GUID guid) {
        return new Builder(guid);
    }
    
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
        
        private Builder(final GUID guid) {
            this.guid = guid;
        }
        
        public ObjectData build() {
            return new ObjectData(guid, objectName, type, creator, copier, module, method,
                    commitHash, moduleVersion, md5, timestamp, parentData, data, keyProps);
        }
        
        public Builder withNullableObjectName(final String objectName) {
            if (!Utils.isNullOrEmpty(objectName)) {
                this.objectName = objectName;
            }
            return this;
        }
        
        public Builder withNullableType(final SearchObjectType type) {
            if (type != null) {
                this.type = type;
            }
            return this;
        }
        
        public Builder withNullableCreator(final String creator) {
            if (!Utils.isNullOrEmpty(creator)) {
                this.creator = creator;
            }
            return this;
        }
        
        public Builder withNullableCopier(final String copier) {
            if (!Utils.isNullOrEmpty(copier)) {
                this.copier = copier;
            }
            return this;
        }
        
        public Builder withNullableModule(final String module) {
            if (!Utils.isNullOrEmpty(module)) {
                this.module = module;
            }
            return this;
        }
        
        public Builder withNullableMethod(final String method) {
            if (!Utils.isNullOrEmpty(method)) {
                this.method = method;
            }
            return this;
        }
        
        public Builder withNullableCommitHash(final String commitHash) {
            if (!Utils.isNullOrEmpty(commitHash)) {
                this.commitHash = commitHash;
            }
            return this;
        }
        
        public Builder withNullableModuleVersion(final String moduleVersion) {
            if (!Utils.isNullOrEmpty(moduleVersion)) {
                this.moduleVersion = moduleVersion;
            }
            return this;
        }
        
        public Builder withNullableMD5(final String md5) {
            if (!Utils.isNullOrEmpty(md5)) {
                this.md5 = md5;
            }
            return this;
        }
        
        public Builder withNullableTimestamp(final Instant timestamp) {
            if (timestamp != null) {
                this.timestamp = timestamp;
            }
            return this;
        }
        
        public Builder withNullableParentData(final Object parentData) {
            if (parentData != null) {
                this.parentData = parentData;
            }
            return this;
        }
        
        public Builder withNullableData(final Object data) {
            if (data != null) {
                this.data = data;
            }
            return this;
        }
        
        public Builder withKeyProperty(final String key, final String property) {
            Utils.notNullOrEmpty(key, "key cannot be null or whitespace");
            keyProps.put(key, property);
            return this;
        }
    }
}
