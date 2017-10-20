package kbasesearchengine.queue;

import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.events.exceptions.FatalIndexingException;
import kbasesearchengine.events.exceptions.FatalRetriableIndexingException;

public interface StatusEventIterator {

    public boolean hasNext();

    public StoredStatusEvent next() throws FatalIndexingException, FatalRetriableIndexingException;
}
