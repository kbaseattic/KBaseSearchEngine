package kbasesearchengine.queue;

import java.io.IOException;

import kbasesearchengine.events.ObjectStatusEvent;
import kbasesearchengine.events.exceptions.FatalIndexingException;
import kbasesearchengine.events.exceptions.FatalRetriableIndexingException;

public interface ObjectStatusEventIterator {

    public boolean hasNext();

    public ObjectStatusEvent next() throws FatalIndexingException, FatalRetriableIndexingException;

    public void markAsVisited(boolean isIndexed) throws IOException;

}
