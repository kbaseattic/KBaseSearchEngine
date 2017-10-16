package kbasesearchengine.queue;

import java.io.IOException;

import kbasesearchengine.events.StatusEvent;
import kbasesearchengine.events.exceptions.FatalIndexingException;
import kbasesearchengine.events.exceptions.FatalRetriableIndexingException;

public interface StatusEventIterator {

    public boolean hasNext();

    public StatusEvent next() throws FatalIndexingException, FatalRetriableIndexingException;

    public void markAsVisited(boolean isIndexed) throws IOException;

}
