package kbaserelationengine.search;

import java.util.Map;

import kbaserelationengine.common.GUID;

public class ObjectData {
    public GUID guid;
    public GUID parentGuid;
    public String objectName;
    public long timestamp;
    public Object parentData;
    public Object data;
    public Map<String, String> keyProps;

    @Override
    public String toString() {
        return "ObjectData [guid=" + guid + ", parentGuid=" + parentGuid
                + ", objectName=" + objectName + ", timestamp=" + timestamp
                + ", parentData=" + parentData + ", data=" + data
                + ", keyProps=" + keyProps + "]";
    }
}
