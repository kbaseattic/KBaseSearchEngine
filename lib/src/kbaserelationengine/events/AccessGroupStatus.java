package kbaserelationengine.events;

public class AccessGroupStatus implements Comparable<AccessGroupStatus>{
	private String _id;
    private String storageCode;
    private Integer accessGroupId;
    private Long timestamp;
    private String[] users;
    
	public AccessGroupStatus(String _id, String storageCode, Integer accessGroupId, Long timestamp, String[] users) {
		super();
		this._id = _id;
		this.storageCode = storageCode;
		this.accessGroupId = accessGroupId;
		this.timestamp = timestamp;
		this.users = users;
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

	public String[] getUsers() {
		return users;
	}

	@Override
	public int compareTo(AccessGroupStatus o) {
		return accessGroupId.compareTo(o.accessGroupId);
	}
    
}
