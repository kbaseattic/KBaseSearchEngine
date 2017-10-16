package kbasesearchengine.events.storage;

import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventWithID;

public interface OldStatusEventStorage {
	
	public StatusEventWithID store(StatusEvent obj);
	
	public void markAsProcessed(StatusEventWithID row, boolean isIndexed);
	
	public StatusEventCursor cursor(String storageCode, boolean processed, int pageSize, String timeAlive);
	
	public boolean nextPage(StatusEventCursor cursor, int nRemovedItems);
}
