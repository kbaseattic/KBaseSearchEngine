package kbaserelationengine.events;

import kbaserelationengine.common.GUID;
import kbaserelationengine.system.StorageObjectType;

public class ObjectStatusEvent {
    private final String _id;
    private final String storageCode;
    private final Integer accessGroupId;
    private final String accessGroupObjectId;
    private final Integer version;
    private final Integer targetAccessGroupId;
    private final Long timestamp;
    private final StorageObjectType storageObjectType;
    private final ObjectStatusEventType eventType;
    private final Boolean isGlobalAccessed;
    private final String newName;


    //TODO BUILDER
    //TODO JAVADOC
    //TODO TEST
    
    public ObjectStatusEvent(
            final String _id,
            final String storageCode,
            final Integer accessGroupId,
            final String accessGroupObjectId,
            final Integer version,
            final String newName,
            final Integer targetAccessGroupId,
            final Long timestamp,
            final StorageObjectType storageObjectType,
            final ObjectStatusEventType eventType,
            final Boolean isGlobalAccessed) {
        super();
        this._id = _id;
        if (storageObjectType != null && storageCode != null &&
                !storageObjectType.getStorageCode().equals(storageCode)) {
            throw new IllegalArgumentException(
                    "Specified mismatched storage code and storage type");
        }
        this.storageCode = storageCode;
        this.accessGroupId = accessGroupId;
        this.accessGroupObjectId = accessGroupObjectId;
        this.version = version;
        this.targetAccessGroupId = targetAccessGroupId;
        this.timestamp = timestamp;
        this.storageObjectType = storageObjectType;
        this.eventType = eventType;
        this.isGlobalAccessed = isGlobalAccessed;
        this.newName = newName;
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ObjectStatusEvent [_id=");
        builder.append(_id);
        builder.append(", storageCode=");
        builder.append(storageCode);
        builder.append(", accessGroupId=");
        builder.append(accessGroupId);
        builder.append(", accessGroupObjectId=");
        builder.append(accessGroupObjectId);
        builder.append(", version=");
        builder.append(version);
        builder.append(", targetAccessGroupId=");
        builder.append(targetAccessGroupId);
        builder.append(", timestamp=");
        builder.append(timestamp);
        builder.append(", storageObjectType=");
        builder.append(storageObjectType);
        builder.append(", eventType=");
        builder.append(eventType);
        builder.append(", isGlobalAccessed=");
        builder.append(isGlobalAccessed);
        builder.append(", newName=");
        builder.append(newName);
        builder.append("]");
        return builder.toString();
    }

    public GUID toGUID(){
        return new GUID(getStorageCode(), accessGroupId, accessGroupObjectId, version, null, null);
    }

    public String getId() {
        return _id;
    }

    public String getStorageCode() {
        return storageObjectType == null ? storageCode : storageObjectType.getStorageCode();
    }


    public Integer getAccessGroupId() {
        return accessGroupId;
    }

    public String getAccessGroupObjectId() {
        return accessGroupObjectId;
    }

    public Integer getVersion() {
        return version;
    }

    public StorageObjectType getStorageObjectType() {
        return storageObjectType;
    }

    public ObjectStatusEventType getEventType() {
        return eventType;
    }

    public Integer getTargetAccessGroupId() {
        return targetAccessGroupId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Boolean isGlobalAccessed(){
        return isGlobalAccessed;
    }

    public String getNewName() {
        return newName;
    }

}
