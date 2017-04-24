package kbaserelationengine.events;

import java.io.IOException;
import java.util.List;

public interface AccessGroupProvider {
	public List<Integer> findAccessGroupIds(String storageCode, String user) throws IOException;	

}
