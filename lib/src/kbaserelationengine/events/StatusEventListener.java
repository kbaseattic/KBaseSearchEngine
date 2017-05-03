package kbaserelationengine.events;

import java.io.IOException;
import java.util.List;


public interface StatusEventListener {
	public void objectStatusChanged(List<ObjectStatusEvent> events) throws IOException;

	public void groupStatusChanged(List<AccessGroupStatus> newStatuses) throws IOException;
	
	public void groupPermissionsChanged(List<AccessGroupStatus> newStatuses) throws IOException;
}
