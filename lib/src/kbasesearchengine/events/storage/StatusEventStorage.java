package kbasesearchengine.events.storage;

import java.io.IOException;

import kbasesearchengine.events.StatusEvent;

public interface StatusEventStorage {
	
	public void store(StatusEvent obj) throws IOException;
	
	public void markAsProcessed(StatusEvent row, boolean isIndexed) throws IOException;
	
	public StatusEventCursor cursor(String storageCode, boolean processed, int pageSize, String timeAlive);
	
	public boolean nextPage(StatusEventCursor cursor, int nRemovedItems);
}
