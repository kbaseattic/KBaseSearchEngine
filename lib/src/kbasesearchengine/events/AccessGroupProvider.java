package kbasesearchengine.events;

import java.io.IOException;
import java.util.List;

public interface AccessGroupProvider {
	public List<Integer> findAccessGroupIds(String user) throws IOException;

}
