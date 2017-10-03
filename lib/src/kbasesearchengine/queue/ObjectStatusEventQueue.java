package kbasesearchengine.queue;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import kbasesearchengine.events.ObjectStatusEvent;
import kbasesearchengine.events.exceptions.FatalIndexingException;
import kbasesearchengine.events.exceptions.FatalRetriableIndexingException;
import kbasesearchengine.events.storage.ObjectStatusCursor;
import kbasesearchengine.events.storage.StatusEventStorage;

public class ObjectStatusEventQueue {
	private final static String BUFFER_ALIVE_TIME = "1m";
	private final static int BUFFER_SIZE = 10;
	private StatusEventStorage objStatusStorage;	
	
	class _Iterator implements ObjectStatusEventIterator{
		String storageCode;
		ObjectStatusCursor cursor = null;
		ObjectStatusEvent[] buffer = new ObjectStatusEvent[BUFFER_SIZE];
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
			for(ObjectStatusEvent row : cursor.getData()){
				buffer[i++] = row;
			}
			nextPos = 0;
			nItems = cursor.getData().size();
		}

		private boolean isBufferEmpty() {
			return !(nextPos < nItems);
		}

		@Override
		public void markAsVisited(boolean isIndexed) throws IOException {
			int curPos = nextPos - 1;
			if (curPos >= 0 && curPos < nItems) {
				objStatusStorage.markAsProcessed(buffer[curPos], isIndexed);
				
			} else {
				throw new IndexOutOfBoundsException();
			}		
			nMarkedAsVisited++;
		}

        @Override
        public ObjectStatusEvent next() {
            if (isBufferEmpty()) {
                throw new NoSuchElementException();
            }
            final ObjectStatusEvent ev = buffer[nextPos++];
            return ev;
        }
	}
		
	public ObjectStatusEventQueue(StatusEventStorage objStatusStorage){
		this.objStatusStorage = objStatusStorage;
	}

    public ObjectStatusEventIterator iterator(String storageCode)
            throws FatalIndexingException, FatalRetriableIndexingException {
        return new _Iterator(storageCode);
    }
	
	public void markAsNonprocessed(String storageCode, String storageObjectType) throws IOException{
		objStatusStorage.markAsNonprocessed(storageCode, storageObjectType);
	}
	
	public int count() throws IOException {
		return count(null);
	}

	public int count(String storageCode) throws IOException {
		return objStatusStorage.count(storageCode, false);
	}

	public List<ObjectStatusEvent> list(int maxSize) throws IOException {
		return list(null, maxSize);
	}

	public List<ObjectStatusEvent> list(String storageCode,
			int maxSize) throws IOException {
		return objStatusStorage.find(storageCode, false, maxSize);
	}		
}
