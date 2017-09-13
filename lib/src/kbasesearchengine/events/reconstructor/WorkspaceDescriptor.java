package kbasesearchengine.events.reconstructor;

import kbasesearchengine.events.AccessGroupStatus;

public class WorkspaceDescriptor implements Comparable<WorkspaceDescriptor> {
	int wsId;
	long timestamp;
	boolean isPrivate;
	boolean isDeleted;
	AccessGroupStatus ag;
	public WorkspaceDescriptor(int wsId, long timestanp, boolean isPrivate, boolean isDeleted) {
		super();
		this.wsId = wsId;
		this.timestamp = timestanp;
		this.isPrivate = isPrivate;
		this.isDeleted = isDeleted;
		this.ag = null;
	}
	@Override
	public int compareTo(WorkspaceDescriptor o) {
		return wsId < o.wsId ? -1 :(wsId > o.wsId ? 1 : 0);
	}
}
