package kbasesearchengine.events.reconstructor;

import kbasesearchengine.events.ObjectStatusEventType;

public class ObjectDescriptor {
	Integer wsId;
	String objId;
	Integer version;
	String dataType;
	long timestamp;
	boolean isDeleted;
	boolean isProcessed;
	
	ObjectStatusEventType evetType;
	
	public ObjectDescriptor(Integer wsId, String objId, Integer version, String dataType, long timestamp, boolean isDeleted) {
		super();
		this.wsId = wsId;
		this.objId = objId;
		this.version = version;
		this.dataType = dataType;
		this.timestamp = timestamp;
		this.isDeleted = isDeleted;
	}			
	
	@Override
	public String toString(){
		return "{ wsId=" + wsId
				+ ", objId=" + objId
				+ ", version=" + version
				+ ", dataType=" + dataType
				+ ", timestamp=" + timestamp
				+ ", isDeleted=" + isDeleted
				+ ", isProcessed=" + isProcessed
				+ ", evetType=" + (evetType != null ? evetType.toString() : null)
				+" }";
	}

	public String toRef() {
		return wsId.toString() + "/" + objId + "/" + version.toString();
	}
}
