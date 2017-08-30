package kbaserelationengine.events;

import java.util.List;

public class AccessGroupStatus implements Comparable<AccessGroupStatus>{
	private String _id;
    private String storageCode;
    private Integer accessGroupId;
    private Long timestamp;
    private Boolean isPrivate;
    private Boolean isDeleted;
    private List<String> users;
    
	public AccessGroupStatus(
	        final String _id,
	        final String storageCode,
	        final Integer accessGroupId,
	        final Long timestamp,
	        final Boolean isPrivate,
	        final Boolean isDeleted,
	        final List<String> users) {
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
				+ " ,users=" + users 
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

	public List<String> getUsers() {
		return users;
	}

	@Override
	public int compareTo(AccessGroupStatus o) {
		return accessGroupId.compareTo(o.accessGroupId);
	}
    
}
