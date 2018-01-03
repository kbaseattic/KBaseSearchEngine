package kbasesearchengine.search;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import kbasesearchengine.common.GUID;
import kbasesearchengine.system.SearchObjectType;
import kbasesearchengine.tools.Utils;

public class ObjectData {
    
    //TODO JAVADOC
    //TODO TESTS
    //TODO NNOW Optionals vs. nulls
    
    private final GUID guid;
    private final GUID parentGuid;
    private final String objectName;
    private final SearchObjectType type;
    private final String creator;
    private final String copier;
    private final String module;
    private final String method;
    private final String commitHash;
    private final String moduleVersion;
    private final String md5;
    private final Instant timestamp;
    private final Object parentData;
    private final Object data;
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
            this.parentGuid = new GUID(guid, null, null);
        } else {
            this.parentGuid = null;
        }
        this.objectName = objectName;
        this.type = type;
        this.creator = creator;
        this.copier = copier;
        this.module = module;
        this.method = method;
        this.commitHash = commitHash;
        this.moduleVersion = moduleVersion;
        this.md5 = md5;
        this.timestamp = timestamp;
        this.parentData = parentData;
        this.data = data;
        this.keyProps = Collections.unmodifiableMap(keyProps);
    }

    public GUID getGUID() {
        return guid;
    }

    public GUID getParentGUID() {
        return parentGuid;
    }

    public String getObjectName() {
        return objectName;
    }

    public SearchObjectType getType() {
        return type;
    }

    public String getCreator() {
        return creator;
    }

    public String getCopier() {
        return copier;
    }

    public String getModule() {
        return module;
    }

    public String getMethod() {
        return method;
    }

    public String getCommitHash() {
        return commitHash;
    }

    public String getModuleVersion() {
        return moduleVersion;
    }

    public String getMd5() {
        return md5;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Object getParentData() {
        return parentData;
    }

    public Object getData() {
        return data;
    }

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
