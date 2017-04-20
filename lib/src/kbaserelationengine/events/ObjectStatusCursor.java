package kbaserelationengine.events;

import java.util.ArrayList;
import java.util.List;

public class ObjectStatusCursor {
	
	private String cursorId;
	private int pageSize;
	private String timeAlive;
	private int pageIndex;
	List<ObjectStatus> data = new ArrayList<ObjectStatus>();
	
	public ObjectStatusCursor(String cursorId, int pageSize, String timeAlive) {
		super();
		this.pageSize = pageSize;
		this.cursorId = cursorId;
		this.timeAlive = timeAlive;
		pageIndex = 0;
	}

	protected void nextPage(List<ObjectStatus> items){
		pageIndex++;
		data.clear();
		data.addAll(items);
	}
	
	public int getPageSize() {
		return pageSize;
	}

	public String getCursorId() {
		return cursorId;
	}

	public String getTimeAlive() {
		return timeAlive;
	}

	public int getPageIndex() {
		return pageIndex;
	}

	public List<ObjectStatus> getData() {
		return data;
	}

	
	
}
