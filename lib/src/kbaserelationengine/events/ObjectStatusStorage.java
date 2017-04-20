package kbaserelationengine.events;

import java.io.IOException;
import java.util.List;

import kbaserelationengine.common.GUID;

public interface ObjectStatusStorage {
	
	public void createStorage() throws IOException;
		
	public void deleteStorage() throws IOException;

	public void store(ObjectStatus obj) throws IOException;
	
	public void markAsProcessed(ObjectStatus row, boolean isIndexed) throws IOException;
	
	public void markAsNonprocessed(String storageCode, String storageObjectType) throws IOException;

	
		
	public int count(String storageCode, boolean processed) throws IOException;
	
	public List<ObjectStatus> find(String storageCode, boolean processed, int maxSize) throws IOException;
	
	public ObjectStatusCursor cursor(String storageCode, boolean processed, int pageSize, String timeAlive) throws IOException;
	
	public boolean nextPage(ObjectStatusCursor cursor) throws IOException;
	
	public List<ObjectStatus> find(String storageCode, boolean processed, List<GUID> guids) throws IOException;

	
}
