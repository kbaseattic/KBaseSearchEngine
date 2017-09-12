package kbaserelationengine.search;

import java.util.Map;

import kbaserelationengine.common.GUID;

public class ObjectData {
    public GUID guid;
    public GUID parentGuid;
    public String objectName;
    public String type;
    public String creator;
    public String copier;
    public String module;
    public String method;
    public String commitHash;
    public String moduleVersion;
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
        builder.append(", version=");
        builder.append(moduleVersion);
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
}
