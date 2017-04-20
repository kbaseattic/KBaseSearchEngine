package kbaserelationengine.queue;

import java.io.IOException;
import java.util.List;

import kbaserelationengine.events.ObjectStatus;
import kbaserelationengine.events.ObjectStatusCursor;
import kbaserelationengine.events.ObjectStatusStorage;

public class IndexingQueue {
	private final static String BUFFER_ALIVE_TIME = "1m";
	private final static int BUFFER_SIZE = 10;
	private ObjectStatusStorage objStatusStorage;	
	
	class _Iterator implements IndexingIterator{
		String storageCode;
		ObjectStatusCursor cursor = null;
		ObjectStatus[] buffer = new ObjectStatus[BUFFER_SIZE];
		int nextPos = 0;
		int nItems = 0;
		

		public _Iterator(String storageCode) {
			super();
			this.storageCode = storageCode;
		}
				

		@Override
		public boolean hasNext() throws IOException {
			if(bufferEmpy()){
				loadBuffer();
			}			
			return !bufferEmpy();
		}

		private void loadBuffer() throws IOException {
			if(cursor == null){
				cursor = objStatusStorage.cursor(storageCode, false, BUFFER_SIZE, BUFFER_ALIVE_TIME);							
			} else{
				objStatusStorage.nextPage(cursor);
			}
						
			int i = 0;
			for(ObjectStatus row : cursor.getData()){
				buffer[i++] = row;
			}
			nextPos = 0;
			nItems = cursor.getData().size();
		}

		private boolean bufferEmpy() {
			return !(nextPos < nItems);
		}

		@Override
		public void markAsVisitied(boolean isIndexed) throws IOException {
			int curPos = nextPos - 1;
			if( curPos >= 0 && curPos < nItems){
				objStatusStorage.markAsProcessed(buffer[curPos], isIndexed);
				
			} else{
				throw new IndexOutOfBoundsException();
			}					
		}

		@Override
		public ObjectStatus next() {
			if(bufferEmpy()){
				throw new IndexOutOfBoundsException();
			}					
			return buffer[nextPos++];
		}		
	}
		
	public IndexingQueue(ObjectStatusStorage objStatusStorage){
		this.objStatusStorage = objStatusStorage;
	}    
    	
	public IndexingIterator iterator(String storageCode){		
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

	public List<ObjectStatus> list(int maxSize) throws IOException {
		return list(null, maxSize);
	}

	public List<ObjectStatus> list(String storageCode,
			int maxSize) throws IOException {
		return objStatusStorage.find(storageCode, false, maxSize);
	}		
}
