package kbasesearchengine.events.storage;

import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.StatusEventProcessingState;
import kbasesearchengine.events.StoredStatusEvent;

public interface OldStatusEventStorage {
	
	public StoredStatusEvent store(StatusEvent obj, StatusEventProcessingState state);
	
	public void markAsProcessed(StoredStatusEvent row, StatusEventProcessingState state);
	
	public StatusEventCursor cursor(String storageCode, boolean processed, int pageSize, String timeAlive);
	
	public boolean nextPage(StatusEventCursor cursor, int nRemovedItems);
}
