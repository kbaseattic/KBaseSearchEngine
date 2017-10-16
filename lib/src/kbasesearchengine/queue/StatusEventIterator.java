package kbasesearchengine.queue;

import kbasesearchengine.events.StatusEventWithID;
import kbasesearchengine.events.exceptions.FatalIndexingException;
import kbasesearchengine.events.exceptions.FatalRetriableIndexingException;

public interface StatusEventIterator {

    public boolean hasNext();

    public StatusEventWithID next() throws FatalIndexingException, FatalRetriableIndexingException;
}
