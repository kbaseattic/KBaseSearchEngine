package kbaserelationengine.events.test.fake;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import kbaserelationengine.common.GUID;
import kbaserelationengine.events.ObjectStatus;
import kbaserelationengine.events.ObjectStatusCursor;
import kbaserelationengine.events.ObjectStatusStorage;

public class FakeObjectStatusStorage implements ObjectStatusStorage {

	@Override
	public int count(String storageCode, boolean processed) throws IOException {
		return 0;
	}

	@Override
	public List<ObjectStatus> find(String storageCode, boolean processed,
			int maxSize) throws IOException {
		return new ArrayList<ObjectStatus>();
	}

	@Override
	public List<ObjectStatus> find(String storageCode, boolean processed,
			List<GUID> guids) throws IOException {
		return new ArrayList<ObjectStatus>();
	}

	@Override
	public void createStorage() throws IOException {
	}

	@Override
	public void deleteStorage() throws IOException {
	}

	@Override
	public void markAsNonprocessed(String storageCode, String storageObjectType)
			throws IOException {		
	}

	@Override
	public void markAsProcessed(ObjectStatus row, boolean isIndexed)
			throws IOException {
	}

	@Override
	public void store(ObjectStatus obj) throws IOException {
	}

	@Override
	public ObjectStatusCursor cursor(String storageCode, boolean processed, int pageSize, String timeAlive)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean nextPage(ObjectStatusCursor cursor) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}
}
