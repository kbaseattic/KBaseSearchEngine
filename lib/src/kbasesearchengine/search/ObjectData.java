package kbasesearchengine.search;

import java.util.Map;

import kbasesearchengine.common.GUID;
import kbasesearchengine.system.SearchObjectType;

public class ObjectData {
    
    //TODO JAVADOC
    //TODO TESTS
    //TODO IMMUTABLE needs a builder? Probably more readable than a huge constructor
    
    public GUID guid;
    public GUID parentGuid;
    public String objectName;
    public SearchObjectType type;
    public String creator;
    public String copier;
    public String module;
    public String method;
    public String commitHash;
    public String moduleVersion;
    public String md5;
    public long timestamp;
    public Object parentData;
    public Object data;
    public Map<String, String> keyProps;

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
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
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
        if (timestamp != other.timestamp) {
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
}
