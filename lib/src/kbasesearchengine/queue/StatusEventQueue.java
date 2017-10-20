package kbasesearchengine.queue;

import java.util.NoSuchElementException;

import kbasesearchengine.events.StoredStatusEvent;
import kbasesearchengine.events.exceptions.FatalIndexingException;
import kbasesearchengine.events.exceptions.FatalRetriableIndexingException;
import kbasesearchengine.events.storage.StatusEventCursor;
import kbasesearchengine.events.storage.OldStatusEventStorage;

public class StatusEventQueue {
	private final static String BUFFER_ALIVE_TIME = "1m";
	private final static int BUFFER_SIZE = 10;
	private OldStatusEventStorage objStatusStorage;	
	
	class _Iterator implements StatusEventIterator{
		String storageCode;
		StatusEventCursor cursor = null;
		StoredStatusEvent[] buffer = new StoredStatusEvent[BUFFER_SIZE];
		int nextPos = 0;
		int nItems = 0;
		int nMarkedAsVisited = 0;
		

		public _Iterator(String storageCode) {
			super();
			this.storageCode = storageCode;
			loadBuffer();
		}

		@Override
		public boolean hasNext() {
			if(isBufferEmpty()){
				loadBuffer();
			}			
			return !isBufferEmpty();
		}

		private void loadBuffer() {
			if (cursor == null){
				cursor = objStatusStorage.cursor(storageCode, false, BUFFER_SIZE, BUFFER_ALIVE_TIME);
			} else {
				objStatusStorage.nextPage(cursor, nMarkedAsVisited);
			}
						
			int i = 0;
			for(StoredStatusEvent row : cursor.getData()){
				buffer[i++] = row;
			}
			nextPos = 0;
			nItems = cursor.getData().size();
		}

		private boolean isBufferEmpty() {
			return !(nextPos < nItems);
		}

        @Override
        public StoredStatusEvent next() {
            if (isBufferEmpty()) {
                throw new NoSuchElementException();
            }
            final StoredStatusEvent ev = buffer[nextPos++];
            return ev;
        }
	}
		
	public StatusEventQueue(OldStatusEventStorage objStatusStorage){
		this.objStatusStorage = objStatusStorage;
	}

    public StatusEventIterator iterator(String storageCode)
            throws FatalIndexingException, FatalRetriableIndexingException {
        return new _Iterator(storageCode);
    }
}
	
