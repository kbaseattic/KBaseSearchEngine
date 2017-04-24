package kbaserelationengine.events;

import java.io.IOException;

public interface StatusEventTrigger {

	public void registerListener(StatusEventListener listener);
	public void trigger(ObjectStatusEvent event) throws IOException;
	public void trigger(AccessGroupStatus newStatus) throws IOException;
	
}
