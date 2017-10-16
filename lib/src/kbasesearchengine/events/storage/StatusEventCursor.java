package kbasesearchengine.events.storage;

import java.util.ArrayList;
import java.util.List;

import kbasesearchengine.events.StatusEventWithID;

public class StatusEventCursor {
	
	private String cursorId;
	private int pageSize;
	private String timeAlive;
	private int pageIndex;
	List<StatusEventWithID> data = new ArrayList<StatusEventWithID>();
	
	public StatusEventCursor(String cursorId, int pageSize, String timeAlive) {
		super();
		this.pageSize = pageSize;
		this.cursorId = cursorId;
		this.timeAlive = timeAlive;
		pageIndex = 0;
	}

	public void nextPage(List<StatusEventWithID> objs){
		pageIndex++;
		data.clear();
		data.addAll(objs);
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

	public List<StatusEventWithID> getData() {
		return data;
	}
}
