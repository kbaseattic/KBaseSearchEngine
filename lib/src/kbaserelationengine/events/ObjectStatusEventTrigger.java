package kbaserelationengine.events;

import java.io.IOException;

public interface ObjectStatusEventTrigger {

	public void registerListener(ObjectStatusEventListener listener);
	public void trigger(ObjectStatusEvent event) throws IOException;
	public void trigger(AccessGroupStatus newStatus) throws IOException;
	
}
