package kbaserelationengine.events.storage;

import java.util.ArrayList;
import java.util.List;

import kbaserelationengine.events.ObjectStatusEvent;

public class ObjectStatusCursor {
	
	private String cursorId;
	private int pageSize;
	private String timeAlive;
	private int pageIndex;
	List<ObjectStatusEvent> data = new ArrayList<ObjectStatusEvent>();
	
	public ObjectStatusCursor(String cursorId, int pageSize, String timeAlive) {
		super();
		this.pageSize = pageSize;
		this.cursorId = cursorId;
		this.timeAlive = timeAlive;
		pageIndex = 0;
	}

	protected void nextPage(List<ObjectStatusEvent> items){
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

	public List<ObjectStatusEvent> getData() {
		return data;
	}

	
	
}
