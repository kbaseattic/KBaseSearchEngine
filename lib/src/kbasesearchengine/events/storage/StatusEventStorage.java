package kbasesearchengine.events.storage;

import java.io.IOException;
import java.util.List;

import kbasesearchengine.events.StatusEvent;

public interface StatusEventStorage {
	
	public void createStorage() throws IOException;
		
	public void deleteStorage() throws IOException;

	public void store(StatusEvent obj) throws IOException;
	
	public void markAsProcessed(StatusEvent row, boolean isIndexed) throws IOException;
	
	public void markAsNonprocessed(String storageCode, String storageObjectType) throws IOException;

	
		
	public int count(String storageCode, boolean processed) throws IOException;
	
	public List<StatusEvent> find(String storageCode, boolean processed, int maxSize) throws IOException;
	
	public StatusEventCursor cursor(String storageCode, boolean processed, int pageSize, String timeAlive);
	
	public boolean nextPage(StatusEventCursor cursor, int nRemovedItems);
	
	public List<StatusEvent> find(String storageCode, int accessGroupId, List<String> accessGroupObjectIds) throws IOException;
		
}
