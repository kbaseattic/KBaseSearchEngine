package kbaserelationengine.events;

import java.io.IOException;


public interface StatusEventListener {
	public void statusChanged(ObjectStatusEvent event) throws IOException;

	public void statusChanged(AccessGroupStatus newStatus) throws IOException;
}
