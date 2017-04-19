package kbaserelationengine.queue;

import java.io.IOException;
import java.util.List;

import kbaserelationengine.events.ObjectStatus;
import kbaserelationengine.events.ObjectStatusEventListener;
import kbaserelationengine.events.ObjectStatusStorage;

public class IndexingQueue implements ObjectStatusEventListener, IndexingIterator{
	private ObjectStatusStorage queueStorage;
	
	public IndexingQueue(ObjectStatusStorage queueStorage){
		this.queueStorage = queueStorage;
	}    
    
	public void statusChanged(ObjectStatus obj) throws IOException {
		queueStorage.store(obj);
	}	
	
	public int count() throws IOException {
		return count(null);
	}

	public int count(String storageCode) throws IOException {
		return queueStorage.countNonIndexedObjects(storageCode);
	}

	public List<ObjectStatus> list(int maxSize) throws IOException {
		return list(null, maxSize);
	}

	public List<ObjectStatus> list(String storageCode,
			int maxSize) throws IOException {
		return queueStorage.find(storageCode, maxSize);
	}

	public ObjectStatus next() throws IOException {
		return next(null);
	}

	public ObjectStatus next(String storageCode) throws IOException {
		List<ObjectStatus> rows = next(storageCode, 1);
		return rows.size() > 0 ? rows.get(0): null;
	}

	public List<ObjectStatus> next(int size) throws IOException {
		return next(null, size);
	}

	public List<ObjectStatus> next(String storageCode, int size) throws IOException {
		List<ObjectStatus> rows = list(storageCode, size);
		for(ObjectStatus row : rows){
			queueStorage.markAsIndexed(row);
		}
		return rows;
	}
	
}
