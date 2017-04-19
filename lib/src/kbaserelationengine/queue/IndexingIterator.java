package kbaserelationengine.queue;

import java.io.IOException;
import java.util.List;

import kbaserelationengine.events.ObjectStatus;

public interface IndexingIterator {
	
	public ObjectStatus next() throws IOException;

	public ObjectStatus next(String storageCode) throws IOException;

	public List<ObjectStatus> next(int size) throws IOException;

	public List<ObjectStatus> next(String storageCode, int size) throws IOException;
		
}
