package kbaserelationengine.events;

import java.io.IOException;
import java.util.List;

import kbaserelationengine.common.GUID;

public interface ObjectStatusStorage {
	
	public void initStorage() throws IOException;
	
	public void store(ObjectStatus obj) throws IOException;
	
	public void markAsIndexed(ObjectStatus row) throws IOException;	
	
	public int countNonIndexedObjects(String storageCode) throws IOException;
	
	public List<ObjectStatus> find(String storageCode, int maxSize) throws IOException;
	
	public List<ObjectStatus> find(String storageCode, List<GUID> guids) throws IOException;
	
}
