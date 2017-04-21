package kbaserelationengine.queue;

import java.io.IOException;

import kbaserelationengine.events.ObjectStatusEvent;

public interface ObjectStatusEventIterator {
	
	public boolean hasNext() throws IOException;
	
	public ObjectStatusEvent next() throws IOException;
	
	public void markAsVisitied(boolean isIndexed) throws IOException;
	
}
