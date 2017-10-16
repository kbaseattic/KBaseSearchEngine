package kbasesearchengine.events.storage;

import java.util.ArrayList;
import java.util.List;

import kbasesearchengine.events.StatusEvent;

public class StatusEventCursor {
	
	private String cursorId;
	private int pageSize;
	private String timeAlive;
	private int pageIndex;
	List<StatusEvent> data = new ArrayList<StatusEvent>();
	
	public StatusEventCursor(String cursorId, int pageSize, String timeAlive) {
		super();
		this.pageSize = pageSize;
		this.cursorId = cursorId;
		this.timeAlive = timeAlive;
		pageIndex = 0;
	}

	public void nextPage(List<StatusEvent> items){
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

	public List<StatusEvent> getData() {
		return data;
	}
}
