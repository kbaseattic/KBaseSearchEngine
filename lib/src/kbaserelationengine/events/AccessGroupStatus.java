package kbaserelationengine.events;

import java.util.Arrays;

public class AccessGroupStatus implements Comparable<AccessGroupStatus>{
	private String _id;
    private String storageCode;
    private Integer accessGroupId;
    private Long timestamp;
    private Boolean isPrivate;
    private Boolean isDeleted;
    private String[] users;
    
	public AccessGroupStatus(String _id, String storageCode, Integer accessGroupId, Long timestamp, Boolean isPrivate, Boolean isDeleted,String[] users) {
		super();
		this._id = _id;
		this.storageCode = storageCode;
		this.accessGroupId = accessGroupId;
		this.timestamp = timestamp;
		
		this.isPrivate = isPrivate;
		this.isDeleted = isDeleted;
		this.users = users;
	}

	@Override
	public String toString(){
		return "{" 
				+ " _id=" + _id
				+ " ,storageCode=" + storageCode
				+ " ,accessGroupId=" + accessGroupId
				+ " ,timestamp=" + timestamp
				+ " ,isPrivate=" + isPrivate
				+ " ,isDeleted=" + isDeleted
				+ " ,users=" + Arrays.toString(users) 
				+ "}";
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

	public Long getTimestamp() {
		return timestamp;
	}
	
	public Boolean isPrivate(){
		return isPrivate;
	}
		
	public Boolean isDeleted(){
		return isDeleted;
	}

	public String[] getUsers() {
		return users;
	}

	@Override
	public int compareTo(AccessGroupStatus o) {
		return accessGroupId.compareTo(o.accessGroupId);
	}
    
}
