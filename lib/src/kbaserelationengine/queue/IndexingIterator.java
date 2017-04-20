package kbaserelationengine.queue;

import java.io.IOException;

import kbaserelationengine.events.ObjectStatus;

public interface IndexingIterator {
	
	public boolean hasNext() throws IOException;
	
	public ObjectStatus next() throws IOException;
	
	public void markAsVisitied(boolean isIndexed) throws IOException;
	
}
