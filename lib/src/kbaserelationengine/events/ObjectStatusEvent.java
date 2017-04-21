package kbaserelationengine.events;

import kbaserelationengine.common.GUID;

public class ObjectStatusEvent {
	private String _id;
    private String storageCode;
    private Integer accessGroupId;
    private String accessGroupObjectId;
    private Integer version;
    private Integer targetAccessGroupId;
    private Long timestamp;
    private String storageObjectType;
    private ObjectStatusEventType eventType;
    
    
	public ObjectStatusEvent(String _id, String storageCode, Integer accessGroupId,
			String accessGroupObjectId, Integer version,
		    Integer targetAccessGroupId,
		    Long timestamp,
			String storageObjectType,
			ObjectStatusEventType eventType) {
		super();
		this._id = _id;
		this.storageCode = storageCode;
		this.accessGroupId = accessGroupId;
		this.accessGroupObjectId = accessGroupObjectId;
		this.version = version;
		this.targetAccessGroupId = targetAccessGroupId;
		this.timestamp = timestamp;
		this.storageObjectType = storageObjectType;
		this.eventType = eventType;
	}

	@Override
	public String toString(){
		return "{" + "_id=" + _id
		+", storageCode=" + storageCode
		+", accessGroupId=" + accessGroupId
		+", accessGroupObjectId=" + accessGroupObjectId
		+", version=" + version
		+", targetAccessGroupId=" + targetAccessGroupId
		+", timestamp=" + timestamp
		+", storageObjectType=" + storageObjectType
		+", eventType=" + eventType.toString();
	}
	
	public GUID toGUID(){
		return new GUID(storageCode, accessGroupId, accessGroupObjectId, version, null, null);
	}

	public String getId() {
		return _id;
	}
	
	public String getStorageCode() {
		return storageCode;
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


	public String getStorageObjectType() {
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
	
	
    
}
